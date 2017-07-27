package scorex.crypto.authds.avltree.batch

import com.google.common.primitives.Longs
import io.iohk.iodb.{ByteArrayWrapper, Store}
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

  override def update(prover: BatchAVLProver[_]): Try[Unit] = Try {
    //TODO topNode is a special case?
    val topNode = prover.topNode
    val key = nodeKey(topNode)
    val topNodePair = (key, ByteArrayWrapper(toBytes(topNode)))
    val digestWrapper = ByteArrayWrapper(prover.digest)
    val indexes = Seq((TopNodeKey, key))
    val toInsert = serializedVisitedNodes(topNode)
    log.trace(s"Put(${store.lastVersionID}) ${toInsert.map(k => Base58.encode(k._1.data))}")
    val toUpdate = if (!toInsert.map(_._1).contains(key)) {
      topNodePair +: (indexes ++ toInsert)
    } else indexes ++ toInsert

    //TODO toRemove list?
    store.update(digestWrapper, Seq(), toUpdate)

  }.recoverWith { case e =>
    log.warn("Failed to update tree", e)
    Failure(e)
  }

  override def rollback(version: Version): Try[(ProverNodes, Int)] = Try {
    store.rollback(ByteArrayWrapper(version))
    def recover(key: Array[Byte]): ProverNodes = {
      val bytes = store(ByteArrayWrapper(key)).data
      bytes.head match {
        case 0 =>
          val balance = bytes.slice(1, 2).head
          val key = bytes.slice(2, 2 + keySize)
          val left = recover(bytes.slice(2 + keySize, 2 + keySize + labelSize))
          val right = recover(bytes.slice(2 + keySize + labelSize, 2 + keySize + (2 * labelSize)))
          val n = new InternalProverNode(key, left, right, balance)
          n.isNew = false
          n
        case 1 =>
          val key = bytes.slice(1, 1 + keySize)
          val value = bytes.slice(1 + keySize, 1 + keySize + valueSize)
          val nextLeafKey = bytes.slice(1 + keySize + valueSize, 1 + (2 * keySize) + valueSize)
          val l = new ProverLeaf(key, value, nextLeafKey)
          l.isNew = false
          l
      }
    }
    val top = recover(store(TopNodeKey).data)

    def height(n:ProverNodes, res: Int = 0): Int = n match {
      case n: InternalProverNode => Math.max(height(n.left, res + 1), height(n.right, res + 1))
      case n: ProverLeaf => res
    }
    top -> height(top)
  }.recoverWith { case e =>
    log.warn("Failed to recover tree", e)
    Failure(e)
  }

  override def version: Version = store.lastVersionID.map(_.data).getOrElse(Array.emptyByteArray)

  override def isEmpty: Boolean = {
    store.lastVersionID.isEmpty
  }

  private def serializedVisitedNodes(node: ProverNodes): Seq[(ByteArrayWrapper, ByteArrayWrapper)] = {
    if (node.isNew) {
      val pair: (ByteArrayWrapper, ByteArrayWrapper) = (nodeKey(node), ByteArrayWrapper(toBytes(node)))
      node match {
        case n: InternalProverNode =>
          val leftSubtree = serializedVisitedNodes(n.left)
          val rightSubtree = serializedVisitedNodes(n.right)
          pair +: (leftSubtree ++ rightSubtree)
        case n: ProverLeaf => Seq(pair)
      }
    } else Seq()
  }

  //TODO label or key???
  private def nodeKey(node: ProverNodes): ByteArrayWrapper = ByteArrayWrapper(node.label)

  private def toBytes(node: ProverNodes): Array[Byte] = node match {
    case n: InternalProverNode => (0: Byte) +: n.balance +: (n.key ++ n.left.label ++ n.right.label)
    case n: ProverLeaf => (1: Byte) +: (n.key ++ n.value ++ n.nextLeafKey)
  }
}
