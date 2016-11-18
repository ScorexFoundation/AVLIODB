package io.iohk.avliodb

import java.io.File

import io.iohk.iodb.LSMStore
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.authds.avltree.batch._
import scorex.crypto.hash.Blake2b256Unsafe
import scorex.utils.Random

class VersionedIODBAVLStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {


  val KL = 26
  val VL = 8
  val LL = 32


  implicit val hf = new Blake2b256Unsafe
  val filename = "/tmp/avliodb"
  new File(filename).listFiles().foreach(f => f.delete())
  new File(filename).mkdir()
  val store = new LSMStore(new File(filename))

  property("Persistence AVL batch prover") {

    val storage = new VersionedIODBAVLStorage(store, KL, VL, LL)
    require(storage.isEmpty)
    val prover = new PersistentBatchAVLProver(new BatchAVLProver(None, KL, VL), storage)
    var digest = prover.rootHash

    //    forAll(kvGen) { case (aKey, aValue) =>
    val aKey = Random.randomBytes(KL)
    val aValue = Random.randomBytes(VL)
    val m = Insert(aKey, aValue)
    prover.performOneModification(m)
    val pf = prover.generateProof
    val verifier = new BatchAVLVerifier(digest, pf, LL, KL, VL)
    verifier.verifyOneModification(m)
    prover.rootHash should not equal digest
    prover.rootHash shouldEqual verifier.digest.get

    prover.rollback(digest).get
    prover.rootHash shouldEqual digest
    prover.performOneModification(m)
    prover.generateProof
    digest = prover.rootHash
    //    }
    //
    //    val prover2 = new PersistentBatchAVLProver(new BatchAVLProver(None, KL, VL), storage)
    //    prover2.rootHash shouldEqual prover.rootHash
  }


  def kvGen: Gen[(Array[Byte], Array[Byte])] = for {
    key <- Gen.listOfN(KL, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KL)(-1: Byte)) && !(k sameElements Array.fill(KL)(0: Byte)) && k.length == KL)
    value <- Gen.listOfN(VL, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (key, value)

}
