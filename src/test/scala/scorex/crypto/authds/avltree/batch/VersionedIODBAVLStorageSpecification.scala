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

  val KeyLength = 26
  val ValueLength = 8
  val LabelLength = 32

  implicit val hf = new Blake2b256Unsafe

  val dirname = "/tmp/iohk/avliodb"
  new File(dirname).mkdirs()
  new File(dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(dirname))
  val storage = new VersionedIODBAVLStorage(store, NodeParameters(KeyLength, ValueLength, LabelLength))
  require(storage.isEmpty)
  val prover = new PersistentBatchAVLProver(new BatchAVLProver(KeyLength, Some(ValueLength), None), storage)

  def kvGen: Gen[(Array[Byte], Array[Byte])] = for {
    key <- Gen.listOfN(KeyLength, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KeyLength)(-1: Byte)) && !(k sameElements Array.fill(KeyLength)(0: Byte)) && k.length == KeyLength)
    value <- Gen.listOfN(ValueLength, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (key, value)

  property("Persistence AVL batch prover rollback") {
    (0 until 100) foreach { i =>
      prover.performOneOperation(Insert(Blake2b256("k" + i).take(KeyLength), Blake2b256("v" + i).take(ValueLength)))
    }
    prover.generateProof

    val digest = prover.digest
    (100 until 200) foreach { i =>
      prover.performOneOperation(Insert(Blake2b256("k" + i).take(KeyLength), Blake2b256("v" + i).take(ValueLength)))
    }
    prover.generateProof
    Base58.encode(prover.digest) should not equal Base58.encode(digest)

    prover.rollback(digest).get
    Base58.encode(prover.digest) shouldEqual Base58.encode(digest)

    prover.checkTree(true)
  }

  property("Persistence AVL batch prover - basic test") {

    var digest = prover.digest

    def oneMod(aKey: Array[Byte], aValue: Array[Byte]): Unit = {
      prover.digest shouldEqual digest
      val m = Insert(aKey, aValue)
      prover.performOneOperation(m)
      val pf = prover.generateProof
      val verifier = new BatchAVLVerifier(digest, pf, KeyLength, Some(ValueLength))
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

    val prover2 = new PersistentBatchAVLProver(new BatchAVLProver(KeyLength, Some(ValueLength), None), storage)
    Base58.encode(prover2.digest) shouldBe Base58.encode(prover.digest)
    prover2.checkTree(postProof = true)
  }
}
