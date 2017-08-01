package scorex.crypto.authds.avltree.batch

import java.io.File

import io.iohk.iodb.LSMStore
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.encode.Base58
import scorex.crypto.hash.{Blake2b256, Blake2b256Unsafe}

import scala.util.{Failure, Try}

class VersionedIODBAVLStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {


  val KL = 26
  val VL = 8
  val LL = 32


  implicit val hf = new Blake2b256Unsafe
  val dirname = "/tmp/iohk/avliodb"
  new File(dirname).mkdirs()
  new File(dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(dirname))
  val storage = new VersionedIODBAVLStorage(store, NodeParameters(KL, VL, LL))
  require(storage.isEmpty)
  val prover = new PersistentBatchAVLProver(new BatchAVLProver(KL, Some(VL), None), storage)

  property("Persistence AVL batch prover rollback") {
    (0 until 100) foreach { i =>
      prover.performOneOperation(Insert(Blake2b256("k" + i).take(KL), Blake2b256("v" + i).take(VL)))
    }
    prover.generateProof

    val digest = prover.digest
    (100 until 200) foreach { i =>
      prover.performOneOperation(Insert(Blake2b256("k" + i).take(KL), Blake2b256("v" + i).take(VL)))
    }
    prover.generateProof
    Base58.encode(prover.digest) should not equal Base58.encode(digest)

    prover.rollback(digest).get
    Base58.encode(prover.digest) shouldEqual Base58.encode(digest)

    prover.checkTree(true)
  }

  property("Persistence AVL batch prover") {

    var digest = prover.digest

    def oneMod(aKey: Array[Byte], aValue: Array[Byte]): Unit = {
      prover.digest shouldEqual digest
      val m = Insert(aKey, aValue)
      prover.performOneOperation(m)
      val pf = prover.generateProof
      val verifier = new BatchAVLVerifier(digest, pf, KL, Some(VL))
      verifier.performOneOperation(m)
      Base58.encode(prover.digest) should not equal Base58.encode(digest)
      Base58.encode(prover.digest) shouldEqual Base58.encode(verifier.digest.get)

      prover.rollback(digest).get
      Base58.encode(prover.digest) shouldEqual Base58.encode(digest)
      prover.performOneOperation(m)
      prover.generateProof
      digest = prover.digest
    }

    forAll(kvGen) { case (aKey, aValue) =>
      Try {
        oneMod(aKey, aValue)
      }.recoverWith { case e =>
        e.printStackTrace()
        Failure(e)
      }
    }

    val prover2 = new PersistentBatchAVLProver(new BatchAVLProver(KL, Some(VL), None), storage)
    Base58.encode(prover2.digest) shouldBe Base58.encode(prover.digest)
    prover2.checkTree(true)
  }


  def kvGen: Gen[(Array[Byte], Array[Byte])] = for {
    key <- Gen.listOfN(KL, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KL)(-1: Byte)) && !(k sameElements Array.fill(KL)(0: Byte)) && k.length == KL)
    value <- Gen.listOfN(VL, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (key, value)

}
