package io.iohk.avliodb

import java.io.File

import io.iohk.iodb.LSMStore
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.authds.avltree.batch._
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
  val filename = "/tmp/iohk/avliodb"
  new File(filename).mkdirs()
  new File(filename).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(filename))
  val storage = new VersionedIODBAVLStorage(store, KL, VL, LL)
  require(storage.isEmpty)
  val prover = new PersistentBatchAVLProver(new BatchAVLProver(None, KL, VL), storage)

  property("Persistence AVL batch prover rollback") {
    (0 until 100) foreach { i =>
      prover.performOneModification(Insert(Blake2b256("k" + i).take(KL), Blake2b256("v" + i).take(VL)))
    }
    prover.generateProof

    val digest = prover.rootHash
    (100 until 200) foreach { i =>
      prover.performOneModification(Insert(Blake2b256("k" + i).take(KL), Blake2b256("v" + i).take(VL)))
    }
    prover.generateProof
    prover.rootHash should not equal digest

    prover.rollback(digest)
    prover.rootHash shouldEqual digest

  }

  property("Persistence AVL batch prover") {

    var digest = prover.rootHash

    def oneMod(aKey: Array[Byte], aValue: Array[Byte]): Unit = {
      prover.rootHash shouldEqual digest
      val m = Insert(aKey, aValue)
      prover.performOneModification(m)
      val pf = prover.generateProof.toArray
      val verifier = new BatchAVLVerifier(digest, pf, LL, KL, VL)
      verifier.verifyOneModification(m)
      prover.rootHash should not equal digest
      prover.rootHash shouldEqual verifier.digest.get

      prover.rollback(digest).get
      prover.rootHash shouldEqual digest
      prover.performOneModification(m)
      prover.generateProof
      digest = prover.rootHash
    }

    forAll(kvGen) { case (aKey, aValue) =>
      Try {
        oneMod(aKey, aValue)
      }.recoverWith { case e =>
        e.printStackTrace()
        Failure(e)
      }
    }

    val prover2 = new PersistentBatchAVLProver(new BatchAVLProver(None, KL, VL), storage)
    Base58.encode(prover2.rootHash) shouldBe Base58.encode(prover.rootHash)
  }


  def kvGen: Gen[(Array[Byte], Array[Byte])] = for {
    key <- Gen.listOfN(KL, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KL)(-1: Byte)) && !(k sameElements Array.fill(KL)(0: Byte)) && k.length == KL)
    value <- Gen.listOfN(VL, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (key, value)

}
