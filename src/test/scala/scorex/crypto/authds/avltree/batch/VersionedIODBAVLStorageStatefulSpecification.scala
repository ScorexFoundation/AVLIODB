package scorex.crypto.authds.avltree.batch

import com.google.common.primitives.Longs
import io.iohk.iodb.LSMStore
import org.scalacheck.commands.Commands
import org.scalacheck.{Gen, Prop}
import org.scalatest.PropSpec
import scorex.crypto.hash.Blake2b256Unsafe
import scorex.utils.{Random => RandomBytes}

import scala.util.{Failure, Random, Success, Try}

class VersionedIODBAVLStorageStatefulSpecification extends PropSpec {
  property("IODBAVLStorage: rollback in stateful environment") {
    IODBCommands.property().check
  }
}


object IODBCommands extends Commands {

  val KeyLength = 32
  val ValueLength = 8
  val LabelLength = 32
  val KeepVersions = 1000

  private val MAXIMUM_GENERATED_OPERATIONS = 20
  private val MINIMUM_OPERATIONS_LENGTH = 10

  private val UPDATE_FRACTION = 2
  private val REMOVE_FRACTION = 4

  implicit val hf = new Blake2b256Unsafe

  case class Operations(operations: List[Operation] = List.empty[Operation]) {
    def include(ops: List[Operation]): Operations = Operations(operations ++ ops)
  }

  override type State = Operations
  override type Sut = PersistentBatchAVLProver[Blake2b256Unsafe]

  override def canCreateNewSut(newState: State,
                               initSuts: Traversable[State],
                               runningSuts: Traversable[Sut]): Boolean = true

  override def newSut(state: State): Sut = createStatefulProver

  override def destroySut(sut: Sut): Unit = ()

  override def initialPreCondition(state: State): Boolean = state.operations.isEmpty

  override def genInitialState: Gen[State] = Gen.const(Operations())

  override def genCommand(state: State): Gen[Command] = {
    val appendsCommandsLength = Random.nextInt(MAXIMUM_GENERATED_OPERATIONS) + MINIMUM_OPERATIONS_LENGTH

    val keys = (0 until appendsCommandsLength).map { _ => RandomBytes.randomBytes(KeyLength) }.toList
    val removedKeys = state.operations.filter(_.isInstanceOf[Remove]).map(_.key).distinct
    val prevKeys = state.operations.map(_.key).distinct.filterNot(k1 => removedKeys.exists{k2 => k1.sameElements(k2)})
    val uniqueKeys = keys.filterNot(prevKeys.contains).distinct
    val updateKeys = Random.shuffle(prevKeys).take(safeDivide(prevKeys.length, UPDATE_FRACTION))
    val removeKeys = Random.shuffle(prevKeys).take(safeDivide(prevKeys.length, REMOVE_FRACTION))

    val appendCommands: List[Operation] = uniqueKeys.map { k => Insert(k, Longs.toByteArray(nextPositiveLong)) }
    val updateCommands: List[Operation] = updateKeys.map { k => UpdateLongBy(k, nextPositiveLong) }
    val removeCommands: List[Operation] = removeKeys.map { k => Remove(k) }

    val all = appendCommands ++ updateCommands ++ removeCommands

    Gen.frequency(
      3 -> BackAndForthCheck(all),
      2 -> ApplyAndRollback(all),
      1 -> BackAndForthDoubleCheck(all),
      1 -> SimpleCheck(all)
    )
  }

  private def safeDivide(base: Int, fraction: Int): Int = if (base > fraction) base / fraction else 0

  private def nextPositiveLong: Long = Random.nextInt(Int.MaxValue).toLong

  private def createStatefulProver: PersistentBatchAVLProver[Blake2b256Unsafe] = {
    val store = new LSMStore(dir = randomTempDir, keySize = KeyLength, keepVersions = KeepVersions)
    val prover = new BatchAVLProver(KeyLength, Some(ValueLength))
    val storage = new VersionedIODBAVLStorage(store, NodeParameters(KeyLength, ValueLength, LabelLength))
    require(storage.isEmpty)
    val persistentProver = PersistentBatchAVLProver.create(prover, storage, paranoidChecks = true).get
    persistentProver
  }

  private def randomTempDir: java.io.File = {
    val dir = java.nio.file.Files.createTempDirectory(Random.alphanumeric.take(15).mkString).toFile
    dir.mkdirs()
    dir.deleteOnExit()
    dir
  }

  case class BackAndForthCheck(ops: List[Operation]) extends Command {

    case class ResultData(digest: Array[Byte], postDigest: Array[Byte], proof: Array[Byte], consistent: Boolean)

    override type Result = ResultData

    override def run(sut: PersistentBatchAVLProver[Blake2b256Unsafe]): Result = {
      val digest = sut.digest
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      val proof = sut.generateProof()
      sut.checkTree(postProof = true)

      sut.rollback(digest).get
      val updatedDigest = sut.digest
      require(digest.sameElements(updatedDigest))
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      val sameProof = sut.generateProof()
      sut.checkTree(postProof = true)
      val updatedPostDigest = sut.digest

      ResultData(updatedDigest, updatedPostDigest, proof, proof.sameElements(sameProof))
    }

    override def nextState(state: Operations): Operations = state.include(ops)

    override def preCondition(state: Operations): Boolean = true

    override def postCondition(state: Operations, result: Try[Result]): Prop = {

      val propBoolean = result match {
        case Success(data) =>
          val verifier = new BatchAVLVerifier(data.digest, data.proof, KeyLength, Some(ValueLength))
          ops.foreach(verifier.performOneOperation)
          data.consistent && verifier.digest.exists(_.sameElements(data.postDigest))
        case Failure(_) =>
          false
      }
      Prop.propBoolean(propBoolean)
    }
  }

  case class BackAndForthDoubleCheck(ops: List[Operation]) extends Command {

    case class ResultData(digest: Array[Byte],
                          postDigest: Array[Byte],
                          digest2: Array[Byte],
                          postDigest2: Array[Byte],
                          proof: Array[Byte],
                          proof2: Array[Byte])

    override type Result = ResultData

    override def run(sut: PersistentBatchAVLProver[Blake2b256Unsafe]): Result = {
      val digest = sut.digest
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      val proof = sut.generateProof()
      sut.checkTree(postProof = true)
      val postDigest = sut.digest

      sut.rollback(digest).get
      val digest2 = sut.digest
      require(digest.sameElements(digest2))
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      val proof2 = sut.generateProof()
      sut.checkTree(postProof = true)
      val postDigest2 = sut.digest

      ResultData(digest, postDigest, digest2, postDigest2, proof, proof2)
    }

    override def nextState(state: Operations): Operations = state.include(ops)

    override def preCondition(state: Operations): Boolean = true

    override def postCondition(state: Operations, result: Try[Result]): Prop = {

      val propBoolean = result match {
        case Success(data) =>
          val verifier1 = new BatchAVLVerifier(data.digest, data.proof, KeyLength, Some(ValueLength))
          val verifier2 = new BatchAVLVerifier(data.digest2, data.proof2, KeyLength, Some(ValueLength))
          ops.foreach(verifier1.performOneOperation)
          ops.foreach(verifier2.performOneOperation)
          val verifiedFirstDataSet = verifier1.digest.exists(_.sameElements(data.postDigest))
          val verifiedSecondDataSet = verifier2.digest.exists(_.sameElements(data.postDigest2))
          verifiedFirstDataSet && verifiedSecondDataSet && data.proof.sameElements(data.proof2)
        case Failure(_) =>
          false
      }
      Prop.propBoolean(propBoolean)
    }
  }

  case class ApplyAndRollback(ops: List[Operation]) extends UnitCommand {

    case class ResultData(digest: Array[Byte], postDigest: Array[Byte], proof: Array[Byte], consistent: Boolean)

    override def run(sut: PersistentBatchAVLProver[Blake2b256Unsafe]): Unit = {
      val digest = sut.digest
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      sut.generateProof()
      sut.checkTree(postProof = true)
      sut.rollback(digest).get
      require(digest.sameElements(sut.digest))
    }

    override def nextState(state: Operations): Operations = state

    override def preCondition(state: Operations): Boolean = true

    override def postCondition(state: Operations, result: Boolean): Prop = Prop.propBoolean(result)
  }

  case class SimpleCheck(ops: List[Operation]) extends Command {

    case class ResultData(digest: Array[Byte], postDigest: Array[Byte], proof: Array[Byte])

    override type Result = ResultData

    override def run(sut: PersistentBatchAVLProver[Blake2b256Unsafe]): Result = {
      val digest = sut.digest
      ops.foreach(sut.performOneOperation)
      sut.checkTree(postProof = false)
      val proof = sut.generateProof()
      sut.checkTree(postProof = true)
      val postDigest = sut.digest
      ResultData(digest, postDigest, proof)
    }

    override def nextState(state: Operations): Operations = state.include(ops)

    override def preCondition(state: Operations): Boolean = true

    override def postCondition(state: Operations, result: Try[Result]): Prop = {
      val propBoolean = result match {
        case Success(data) =>
          val verifier = new BatchAVLVerifier(data.digest, data.proof, KeyLength, Some(ValueLength))
          ops.foreach(verifier.performOneOperation)
          verifier.digest.exists(_.sameElements(data.postDigest))
        case Failure(_) =>
          false
      }
      Prop.propBoolean(propBoolean)
    }
  }
}
