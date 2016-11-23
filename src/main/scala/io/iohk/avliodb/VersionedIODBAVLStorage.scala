package io.iohk.avliodb

import com.google.common.primitives.Longs
import io.iohk.iodb.{ByteArrayWrapper, Store}
import scorex.crypto.authds.avltree._
import scorex.crypto.authds.avltree.batch.VersionedAVLStorage
import scorex.crypto.encode.Base58
import scorex.crypto.hash.{Blake2b256, ThreadUnsafeHash}
import scorex.utils.ScryptoLogging

import scala.util.{Failure, Try}

class VersionedIODBAVLStorage(store: Store,
                              keySize: Int = 26,
                              valueSize: Int = 8,
                              labelSize: Int = 32)
                             (implicit val hf: ThreadUnsafeHash) extends VersionedAVLStorage with ScryptoLogging {

  private val TopNodeKey: ByteArrayWrapper = ByteArrayWrapper(Array.fill(labelSize)(123: Byte))

  override def update(topNode: ProverNodes): Try[Unit] = Try {
    //TODO topNode is a special case?
    val topNodePair = (nodeKey(topNode), ByteArrayWrapper(toBytes(topNode)))
    val indexes: Seq[(ByteArrayWrapper, ByteArrayWrapper)] =
      Seq((TopNodeKey, nodeKey(topNode)),
        (versionsReverseKey(store.lastVersion + 1), ByteArrayWrapper(topNode.label)),
        (versionsKey(topNode.label), ByteArrayWrapper(Longs.toByteArray(store.lastVersion + 1))))
    val toInsert = serializedVisitedNodes(topNode)
    val toUpdate = if (!toInsert.map(_._1).contains(nodeKey(topNode))) {
      topNodePair +: (indexes ++ toInsert)
    } else indexes ++ toInsert

    println(s"Put(${store.lastVersion + 1}: ${toUpdate.map(k => Base58.encode(k._1.data))}")
    //TODO toRemove list?
    store.update(store.lastVersion + 1, Seq(), toUpdate)

  }.recoverWith { case e =>
    log.warn("Failed to update tree", e)
    Failure(e)
  }

  override def rollback(version: Version): Try[ProverNodes] = Try {
    val lastVersion = versions(version)

    store.rollback(lastVersion)
    def recover(key: Array[Byte]): ProverNodes = {
      if(store.get(ByteArrayWrapper(key)) == null) {
        println(s"!! failed to get for key ${Base58.encode(key)}")
      }
      val bytes = store.get(ByteArrayWrapper(key)).data
      bytes.head match {
        case 0 =>
          val balance = bytes.slice(1, 2).head
          val key = bytes.slice(2, 2 + keySize)
          val left = recover(bytes.slice(2 + keySize, 2 + keySize + labelSize))
          val right = recover(bytes.slice(2 + keySize + labelSize, 2 + keySize + (2 * labelSize)))
          val n = ProverNode(key, left, right, balance)
          n.isNew = false
          n
        case 1 =>
          val key = bytes.slice(1, 1 + keySize)
          val value = bytes.slice(1 + keySize, 1 + keySize + valueSize)
          val nextLeafKey = bytes.slice(1 + keySize + valueSize, 1 + (2 * keySize) + valueSize)
          val l = Leaf(key, value, nextLeafKey)
          l.isNew = false
          l
      }
    }
    recover(store.get(TopNodeKey).data)
  }.recoverWith { case e =>
    log.warn("Failed to recover tree", e)
    Failure(e)
  }

  override def version: Version = versionsReverse(store.lastVersion)

  override def isEmpty: Boolean = {
    store.lastVersion == 0
  }


  private def serializedVisitedNodes(node: ProverNodes): Seq[(ByteArrayWrapper, ByteArrayWrapper)] = {
    if (node.isNew) {
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

  private def versions(l: Array[Byte]): Long = Longs.fromByteArray(store.get(versionsKey(l)).data)

  private def versionsKey(l: Array[Byte]): ByteArrayWrapper =
    ByteArrayWrapper(Blake2b256("Versions".getBytes ++ l).take(labelSize))

  private def versionsReverseKey(l: Long): ByteArrayWrapper =
    ByteArrayWrapper(Blake2b256("Rev".getBytes ++ Longs.toByteArray(l)).take(labelSize))

  private def versionsReverse(l: Long): Array[Byte] = store.get(versionsReverseKey(l)).data

  private def toBytes(node: ProverNodes): Array[Byte] = node match {
    case n: ProverNode => (0: Byte) +: n.balance +: (n.key ++ n.leftLabel ++ n.rightLabel)
    case n: Leaf => (1: Byte) +: (n.key ++ n.value ++ n.nextLeafKey)
  }
}
