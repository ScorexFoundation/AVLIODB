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
  type LSN = Long
  private val db: DB = createDB(dir, "ldb_main")
  private val undo: DB = createDB(dir, "ldb_undo")
  private var lsn : LSN = getLastLSN
  private val versionLsn : ArrayBuffer.empty[LSN]
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

  private def newLSN(): Array[Byte] = {
	encodeLSN(lsn += 1)
  }

  private def decodeLSN(lsn: Array[Byte]): LSN = {
    ~ByteBuffer.wrap(lsn).getLong
  }

  private def encodeLSN(lsn: LSN): Array[Byte] = {
    val buf = ByteBuffer.allocate(8)
    buf.putLong(~lsn)
    buf.array()
  }

  private def getLastLSN: LSN = {
    val iterator = undo.iterator
    try {
      iterator.seekToFirst()
      if (iterator.hasNext) {
        decodeLSN(iterator.peekNext().getKey)
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
	var lastLSN : LSN = 0
    undo.forEach(entry => {
      val currVersion = deserializeUndo(entry.getValue).versionID
	  lastLSN = decodeLSN(entry.getKey)
      if (!currVersion.equals(lastVersion)) {
        versionLSN += lastLSN
	    versions += currVersion
        lastVersion = currVersion
      }
    })
	versionLsn += lastLsn
	versionLsn = versionLsn.reverse
	versionLsn.remove(versionLsn.size-1)
    versions.reverse
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
	val lastLsn = lsn
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
      if (keepVersions > 0 && lastLsn != lsn) {
        undo.write(undoBatch, writeOptions)
        if (lastVersionID.isEmpty || !versionID.equals(lastVersionID.get)) {
          assert(!versions.contains(versionID))
          versions += versionID
		  versionLsn += lastLsn + 1
          cleanStart(keepVersions)
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
    val deteriorated = versions.size - count
    if (deteriorated > 0) {
	  val fromLSN = versionLsn(0)
	  val tillLSN = versionLsn(deteriorated)
	  val batch = undo.createWriteBatch()
	  try {
	    for (lsn <- fromLSN until tillLSN) {
	      batch.delete(encodeLSN(lsn))
        }
	    undo.write(batch, writeOptions)
      } finally {
	    batch.close()
	  }
	  versions.remove(0, deteriorated)
	  versionLsn.remove(0, deteriorated)
    }
  }

  def clean(count: Int): Unit = {
    lock.writeLock().lock()
	try {
      cleanStart(count)
    } finally {
      lock.writeLock().unlock()
    }
    undo.resumeCompactions()
    db.resumeCompactions()
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
	  val versionIndex = versions.indexOf(versionID)
      if (versionIndex >= 0) {
        val batch = db.createWriteBatch()
        val undoBatch = undo.createWriteBatch()
        val iterator = undo.iterator()
        try {
          var undoing = true
          iterator.seekToFirst()
          while (undoing) {
		    assert(iterator.hasNext)
            val entry = iterator.next()
  	        val undo = deserializeUndo(entry.getValue)
            if (undo.versionID.equals(versionID)) {
              undoing = false
			  lsn = decodeLSN(entry.getKey)
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
        versionLsn.remove(versionIndex+1, versionLsn.size - versionIndex - 1)
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  def rollbackVersions(): Iterable[VersionID] = {
    versions
  }
}
