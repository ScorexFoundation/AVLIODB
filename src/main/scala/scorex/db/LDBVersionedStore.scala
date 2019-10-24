package scorex.db

import scorex.db.LDBFactory.factory
import io.iohk.iodb.Store.{K, V, VersionID}
import io.iohk.iodb.{ByteArrayWrapper, Store}
import org.iq80.leveldb._
import java.nio.ByteBuffer
import java.io._
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import scorex.crypto.encode.Base58

class LDBVersionedStore(val dir: File, val keepVersions: Int = 0) extends Store {
  private val db: DB = createDB(dir, "ldb_main")
  private val undo: DB = createDB(dir, "ldb_undo")
  private var lsn : Long = getLastLSN
  private val versions : ArrayBuffer[VersionID] = getAllVersions
  private val writeOptions = defaultWriteOptions
  private val lock = new ReentrantReadWriteLock()


  private def createDB(dir: File, storeName: String): DB = {
    val op = new Options()
    op.createIfMissing(true)
    factory.open(new File(dir, storeName), op)
  }

  private def defaultWriteOptions = {
    val options = new WriteOptions()
    options.sync(true)
    options
  }

  def get(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      val b = db.get(key.data)
      if (b == null) None else Some(ByteArrayWrapper(b))
    } finally {
      lock.readLock().unlock()
    }
  }

  def getAll(consumer: (K, V) => Unit): Unit = {
    lock.readLock().lock()
    val iterator = db.iterator()
    try {
      iterator.seekToFirst()
      while (iterator.hasNext) {
        val n = iterator.next()
        consumer(ByteArrayWrapper(n.getKey), ByteArrayWrapper(n.getValue))
      }
    } finally {
      iterator.close()
      lock.readLock().unlock()
    }
  }

  private def newLSN() : Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    lsn += 1
    buf.putLong(~lsn)
    buf.array()
  }

  private def getLastLSN: Long = {
    val iterator = undo.iterator
    try {
      iterator.seekToFirst()
      if (iterator.hasNext) {
        ByteBuffer.wrap(iterator.peekNext().getKey).getLong
      } else {
        0
      }
    } finally {
      iterator.close()
    }
  }

  def lastVersionID: Option[VersionID] = {
    lock.readLock().lock()
    try {
      versions.lastOption
    } finally {
      lock.readLock().unlock()
    }
  }

  def versionIDExists(versionID: VersionID): Boolean = {
    lock.readLock().lock()
    try {
      versions.contains(versionID)
    } finally {
      lock.readLock().unlock()
    }
  }

  private def getAllVersions: ArrayBuffer[VersionID] = {
    val versions = ArrayBuffer.empty[VersionID]
    var lastVersion : VersionID = null
    undo.forEach(entry => {
      val currVersion = deserializeUndo(entry.getValue).versionID
      if (!currVersion.equals(lastVersion)) {
        versions += currVersion
        lastVersion = currVersion
      }
    })
    versions
  }

  case class Undo(versionID: VersionID, key: Array[Byte], value : Array[Byte])

  private def serializeUndo(versionID: VersionID, key: Array[Byte], value : Array[Byte]): Array[Byte] = {
    val valueSize = if (value != null) value.length else 0
    val versionSize = versionID.size
    val keySize = key.length
    val packed = new Array[Byte](2 + versionSize + keySize + valueSize)
    assert(keySize <= 0xFF)
    packed(0) = versionSize.asInstanceOf[Byte]
    packed(1) = keySize.asInstanceOf[Byte]
    Array.copy(versionID.data, 0, packed, 2, versionSize)
    Array.copy(key, 0, packed, 2 + versionSize, keySize)
    if (value != null) {
      Array.copy(value, 0, packed, 2 + versionSize + keySize, valueSize)
    }
    packed
  }

  private def deserializeUndo(undo : Array[Byte]): Undo = {
    val versionSize = undo(0) & 0xFF
    val keySize = undo(1) & 0xFF
    val valueSize = undo.length - versionSize - keySize - 2
    val versionID = ByteArrayWrapper(undo.slice(2, 2 + versionSize))
    val key = undo.slice(2 + versionSize, 2 + versionSize + keySize)
    val value = if (valueSize == 0) null else undo.slice(2 + versionSize + keySize, undo.length)
    Undo(versionID, key, value)
  }

  def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    val batch = db.createWriteBatch()
    val undoBatch = undo.createWriteBatch()
    try {
      toRemove.foreach(b => {
        val key = b.data
        batch.delete(key)
        if (keepVersions > 0) {
          val value = db.get(key)
          if (value != null) {
            undoBatch.put(newLSN(), serializeUndo(versionID, key, value))
          }
        }
      })
      for ((k, v) <- toUpdate) {
        val key = k.data
        if (keepVersions > 0) {
          val old = db.get(key)
          undoBatch.put(newLSN(), serializeUndo(versionID, key, old))
        }
        batch.put(key, v.data)
      }
      db.write(batch, writeOptions)
      if (keepVersions > 0) {
        undo.write(undoBatch, writeOptions)
        if (lastVersionID.isEmpty || !versionID.equals(lastVersionID.get)) {
          assert(!versions.contains(versionID))
          versions += versionID
          //cleanStart(keepVersions)
        }
      }
    } finally {
      // Make sure you close the batch to avoid resource leaks.
      batch.close()
      undoBatch.close()
      lock.writeLock().unlock()
    }
  }

  private def cleanStart(count: Int): Unit = {
    if (versions.size > count) {
      rollbackTo(versions(count))
    }
  }

  def clean(count: Int): Unit = {
    lock.writeLock().lock()
	try {
      cleanStart(count)
      undo.resumeCompactions()
      db.resumeCompactions()
    } finally {
      lock.writeLock().unlock()
    }
  }

  def cleanStop(): Unit = {
    undo.suspendCompactions()
    db.suspendCompactions()
  }

  def close(): Unit = {
    lock.writeLock().lock()
	try {
      undo.close()
      db.close()
    } finally {
      lock.writeLock().unlock()
    }
  }

  def rollback(versionID: VersionID): Unit = {
    lock.writeLock().lock()
	try {
	  rollbackTo(versionID)
    } finally {
      lock.writeLock().unlock()
    }
  }

  private def rollbackTo(versionID: VersionID): Unit = {
    val versionIndex = versions.indexOf(versionID)
    if (versionIndex >= 0) {
      var undoing = true
      val batch = db.createWriteBatch()
      val undoBatch = undo.createWriteBatch()
      val iterator = undo.iterator()
      try {
        iterator.seekToFirst()
        while (undoing && iterator.hasNext) {
          val entry = iterator.next()
		  val undo = deserializeUndo(entry.getValue)
          if (undo.versionID.equals(versionID)) {
            undoing = false
          } else {
            undoBatch.delete(entry.getKey)
            if (undo.value == null) {
              batch.delete(undo.key)
            } else {
              batch.put(undo.key, undo.value)
            }
		  }
        }
        db.write(batch, writeOptions)
        undo.write(undoBatch, writeOptions)
      } finally {
        // Make sure you close the batch to avoid resource leaks.
        iterator.close()
        batch.close()
        undoBatch.close()
      }
      versions.remove(versionIndex+1, versions.size - versionIndex - 1)
	  //System.out.println("Last version = " + versions.last)
    }
  }

  def rollbackVersions(): Iterable[VersionID] = {
    versions
  }
}
