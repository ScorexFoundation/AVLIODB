package scorex.crypto.authds.avltree.batch

import com.google.common.primitives.Ints
import io.iohk.iodb.{ByteArrayWrapper, Store}
import scorex.crypto.authds.avltree.AVLKey
import scorex.crypto.encode.Base58
import scorex.crypto.hash.ThreadUnsafeHash
import scorex.utils.ScryptoLogging

import scala.util.{Failure, Try}

import VersionedIODBAVLStorage.{InternalNodePrefix, LeafPrefix}

class VersionedIODBAVLStorage(store: Store, nodeParameters: NodeParameters)
                             (implicit val hf: ThreadUnsafeHash) extends VersionedAVLStorage with ScryptoLogging {

  private lazy val labelSize = nodeParameters.labelSize

  private val TopNodeKey: ByteArrayWrapper = ByteArrayWrapper(Array.fill(labelSize)(123: Byte))
  private val TopNodeHeight: ByteArrayWrapper = ByteArrayWrapper(Array.fill(labelSize)(124: Byte))

  override def update(prover: BatchAVLProver[_]): Try[Unit] = Try {
    //TODO topNode is a special case?
    val topNode = prover.topNode
    val key = nodeKey(topNode)
    val topNodePair = (key, ByteArrayWrapper(toBytes(topNode)))
    val digestWrapper = ByteArrayWrapper(prover.digest)
    val indexes = Seq(TopNodeKey -> key, TopNodeHeight -> ByteArrayWrapper(Ints.toByteArray(prover.rootNodeHeight)))
    val toInsert = serializedVisitedNodes(topNode)
    log.trace(s"Put(${store.lastVersionID}) ${toInsert.map(k => Base58.encode(k._1.data))}")
    val toUpdate = if (!toInsert.map(_._1).contains(key)) {
      topNodePair +: (indexes ++ toInsert)
    } else indexes ++ toInsert

    println(toUpdate.size + " elements to insert into db")

    //TODO toRemove list?
    store.update(digestWrapper, toRemove = Seq(), toUpdate)
  }.recoverWith { case e =>
    log.warn("Failed to update tree", e)
    Failure(e)
  }

  override def rollback(version: Version): Try[(ProverNodes, Int)] = Try {
    store.rollback(ByteArrayWrapper(version))

    val top = VersionedIODBAVLStorage.fetch(store.get(TopNodeKey).get.data)(hf, store, nodeParameters)
    val topHeight = Ints.fromByteArray(store.get(TopNodeHeight).get.data)

    top -> topHeight
  }.recoverWith { case e =>
    log.warn("Failed to recover tree", e)
    Failure(e)
  }

  override def version: Option[Version] = store.lastVersionID.map(_.data)

  private def serializedVisitedNodes(node: ProverNodes): Seq[(ByteArrayWrapper, ByteArrayWrapper)] = {
    if (node.isNew) {
      val pair: (ByteArrayWrapper, ByteArrayWrapper) = (nodeKey(node), ByteArrayWrapper(toBytes(node)))
      node match {
        case n: InternalProverNode =>
          val leftSubtree = serializedVisitedNodes(n.left)
          val rightSubtree = serializedVisitedNodes(n.right)
          pair +: (leftSubtree ++ rightSubtree)
        case _: ProverLeaf => Seq(pair)
      }
    } else Seq()
  }

  //TODO label or key???
  private def nodeKey(node: ProverNodes): ByteArrayWrapper = ByteArrayWrapper(node.label)

  private def toBytes(node: ProverNodes): Array[Byte] = node match {
    case n: InternalProverNode => InternalNodePrefix +: n.balance +: (n.key ++ n.left.label ++ n.right.label)
    case n: ProverLeaf => LeafPrefix +: (n.key ++ n.value ++ n.nextLeafKey)
  }

  override def rollbackVersions: Iterable[Version] = store.rollbackVersions().map(_.data)

  def leafsIterator() = store.getAll().filter{case (_, v) => v.data.head == LeafPrefix}
}


object VersionedIODBAVLStorage {
  val InternalNodePrefix: Byte = 0: Byte
  val LeafPrefix: Byte = 1: Byte

  def fetch(key: AVLKey)(implicit hf: ThreadUnsafeHash,
                         store: Store,
                         nodeParameters: NodeParameters): ProverNodes = {
    val bytes = store(ByteArrayWrapper(key)).data
    lazy val keySize = nodeParameters.keySize
    lazy val labelSize = nodeParameters.labelSize
    lazy val valueSize = nodeParameters.valueSize

    bytes.head match {
      case InternalNodePrefix =>
        val balance = bytes.slice(1, 2).head
        val key = bytes.slice(2, 2 + keySize)
        val leftKey = bytes.slice(2 + keySize, 2 + keySize + labelSize)
        val rightKey = bytes.slice(2 + keySize + labelSize, 2 + keySize + (2 * labelSize))

        val n = new ProxyInternalProverNode(key, leftKey, rightKey, balance)
        n.isNew = false
        n
      case LeafPrefix =>
        val key = bytes.slice(1, 1 + keySize)
        val value = bytes.slice(1 + keySize, 1 + keySize + valueSize)
        val nextLeafKey = bytes.slice(1 + keySize + valueSize, 1 + (2 * keySize) + valueSize)
        val l = new ProverLeaf(key, value, nextLeafKey)
        l.isNew = false
        l
    }
  }
}