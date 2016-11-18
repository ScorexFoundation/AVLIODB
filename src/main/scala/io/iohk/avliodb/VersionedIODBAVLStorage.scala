package io.iohk.avliodb

import io.iohk.iodb.{ByteArrayWrapper, Store}
import scorex.crypto.authds.avltree._
import scorex.crypto.authds.avltree.batch.VersionedAVLStorage
import scorex.crypto.encode.Base58
import scorex.crypto.hash.ThreadUnsafeHash

import scala.collection.mutable
import scala.util.Try

class VersionedIODBAVLStorage(store: Store,
                              keySize: Int = 26,
                              valueSize: Int = 8,
                              labelSize: Int = 32)(implicit val hf: ThreadUnsafeHash) extends VersionedAVLStorage {

  private val TopNodeKey: ByteArrayWrapper = ByteArrayWrapper(Array.fill(labelSize)(123: Byte))
  println("???" + store.lastVersion)


  override def update(topNode: ProverNodes): Try[Unit] = Try {
    println("Update: "  + Base58.encode(topNode.label))
    val toInsert = serializedVisitedNodes(topNode)
    //TODO to remove?
    store.update(longVersion(topNode.label, true), Seq(), (TopNodeKey, nodeKey(topNode)) +: toInsert)
  }

  override def rollback(version: Version): Try[ProverNodes] = Try {
    println("Rollback: " + Base58.encode(version))

    store.rollback(longVersion(version, false))
    val topNodeBytes = store.get(TopNodeKey).data
    def recover(bytes: Array[Byte]): ProverNodes = bytes.head match {
      case 0 =>
        val balance = bytes.slice(1, 2).head
        val key = bytes.slice(2, 2 + keySize)
        val left = recover(bytes.slice(2 + keySize, 2 + keySize + labelSize))
        val right = recover(bytes.slice(2 + keySize + labelSize, 2 + keySize + (2 * labelSize)))
        ProverNode(key, left, right, balance)
      case 1 =>
        val key = bytes.slice(1, 1 + keySize)
        val value = bytes.slice(1 + keySize, 1 + keySize + valueSize)
        val nextLeafKey = bytes.slice(1 + keySize + valueSize, 1 + (2 * keySize) + valueSize)
        Leaf(key, value, nextLeafKey)
    }
    recover(topNodeBytes)
  }

  override def version: Version = versionsReverse(store.lastVersion)

  override def isEmpty: Boolean = {
    store.lastVersion == 0
  }


  private def serializedVisitedNodes(node: ProverNodes): Seq[(ByteArrayWrapper, ByteArrayWrapper)] = {
    //TODO visited or isNew?
    if (node.visited) {
      val pair: (ByteArrayWrapper, ByteArrayWrapper) = (nodeKey(node), ByteArrayWrapper(toBytes(node)))
      node match {
        case n: ProverNode =>
          val leftSubtree = serializedVisitedNodes(n.left)
          val rightSubtree = serializedVisitedNodes(n.right)
          pair +: (leftSubtree ++ rightSubtree)
        case n: Leaf => Seq(pair)
      }
    } else Seq()
  }

  //TODO label or key???
  private def nodeKey(node: ProverNodes): ByteArrayWrapper = ByteArrayWrapper(node.label)


  //TODO remove when version will be Array[Byte]
  private val InitV = Array.fill(labelSize)(0: Byte)
  private val versions: mutable.Map[mutable.WrappedArray[Byte], Long] = mutable.Map(wrapByteArray(InitV) -> 0L)
  private val versionsReverse: mutable.Map[Long, Array[Byte]] = mutable.Map(0L -> InitV)
  private var lastVersion = store.lastVersion

  private def longVersion(b: Version, isNew: Boolean): Long = {
    println(s"longVersion $lastVersion, $isNew, ${Base58.encode(b)} ")

    if (isNew) {
      lastVersion = lastVersion + 1
      versions.put(b, lastVersion)
      lastVersion
    } else {
      lastVersion = versions(b)
      versions(b)
    }
  }

  private def toBytes(node: ProverNodes): Array[Byte] = node match {
    case n: ProverNode => (0: Byte) +: n.balance +: (n.key ++ n.leftLabel ++ n.rightLabel)
    case n: Leaf => (1: Byte) +: (n.key ++ n.value ++ n.nextLeafKey)
  }
}
