package scorex.crypto.authds.avltree.batch

import io.iohk.iodb.Store
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.authds.avltree.batch.helpers.TestHelper
import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.hash.{Blake2b256, Blake2b256Unsafe, Digest32}
import scorex.utils.{Random => RandomBytes}

import scala.util.Try

class VersionedIODBAVLStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers
  with TestHelper {

  override protected val KL = 32
  override protected val VL = 8
  override protected val LL = 32

  def kvGen: Gen[(ADKey, ADValue)] = for {
    key <- Gen.listOfN(KL, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KL)(-1: Byte)) && !(k sameElements Array.fill(KL)(0: Byte)) && k.length == KL)
    value <- Gen.listOfN(VL, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (ADKey @@ key, ADValue @@ value)


  /**
    * List of all test cases
    */

  val rollbackTest: PERSISTENT_PROVER => Unit = { (prover: PERSISTENT_PROVER) =>

    def ops(s: Int, e: Int): Unit = (s until e).foreach{ i =>
      prover.performOneOperation(Insert(ADKey @@ Blake2b256("k" + i).take(KL),
        ADValue @@ Blake2b256("v" + i).take(VL)))
    }

    ops(0, 100)
    prover.generateProof

    val digest = prover.digest
    val digest58String = digest.toBase58

    ops(100, 200)
    prover.generateProof

    prover.digest.toBase58 should not equal digest58String

    prover.rollback(digest)

    prover.digest.toBase58 shouldEqual digest58String

    prover.checkTree(true)
  }

  val basicTest: (PERSISTENT_PROVER, STORAGE) => Unit = { (prover: PERSISTENT_PROVER, storage: STORAGE) =>
    var digest = prover.digest

    def oneMod(aKey: ADKey, aValue: ADValue): Unit = {
      prover.digest shouldBe digest

      val m = Insert(aKey, aValue)
      prover.performOneOperation(m)
      val pf = prover.generateProof

      val verifier = createVerifier(digest, pf)
      verifier.performOneOperation(m).isSuccess shouldBe true
      prover.digest.toBase58 should not equal digest.toBase58
      prover.digest.toBase58 shouldEqual verifier.digest.get.toBase58

      prover.rollback(digest).get

      prover.checkTree(true)

      prover.digest shouldBe digest

      prover.performOneOperation(m)
      val pf2 = prover.generateProof

      pf shouldBe pf2

      prover.checkTree(true)

      val verifier2 = createVerifier(digest, pf2)
      verifier2.performOneOperation(m).isSuccess shouldBe true

      digest = prover.digest
    }

    (1 to 100).foreach { _ =>
      val (aKey, aValue) = kvGen.sample.get
      oneMod(aKey, aValue)
    }

    val prover2 = createPersistentProver(storage)
    prover2.digest.toBase58 shouldEqual prover.digest.toBase58
    prover2.checkTree(postProof = true)
  }

  val rollbackVersionsTest: (PERSISTENT_PROVER, STORAGE) => Unit = { (prover: PERSISTENT_PROVER, storage: STORAGE) =>
    (0L until 50L).foreach { long =>
      val insert = Insert(ADKey @@ RandomBytes.randomBytes(32),
        ADValue @@ com.google.common.primitives.Longs.toByteArray(long))
      prover.performOneOperation(insert)
      prover.generateProof()
      prover.digest
    }
    noException should be thrownBy storage.rollbackVersions.foreach(v => prover.rollback(v).get)
  }

  def removeFromLargerSetSingleRandomElementTest(createStore: (Int) => Store): Unit = {
    val minSetSize = 10000
    val maxSetSize = 200000

    forAll(Gen.choose(minSetSize, maxSetSize), Arbitrary.arbBool.arbitrary) { case (cnt, generateProof) =>
      whenever(cnt > minSetSize) {

        val store = createStore(0).ensuring(_.lastVersionID.isEmpty)
        val t = Try {
          var keys = IndexedSeq[ADKey]()
          val p = new BatchAVLProver[Digest32, Blake2b256Unsafe](KL, Some(VL))

          (1 to cnt) foreach { _ =>
            val key = ADKey @@ RandomBytes.randomBytes(KL)
            val value = ADValue @@ RandomBytes.randomBytes(VL)

            keys = key +: keys

            p.performOneOperation(Insert(key, value)).isSuccess shouldBe true
            p.unauthenticatedLookup(key).isDefined shouldBe true
          }

          if (generateProof) p.generateProof()
          val storage = createVersionedStorage(store)
          assert(storage.isEmpty)

          val prover = createPersistentProver(storage, p)

          val keyPosition = scala.util.Random.nextInt(keys.length)
          val rndKey = keys(keyPosition)

          prover.unauthenticatedLookup(rndKey).isDefined shouldBe true
          val removalResult = prover.performOneOperation(Remove(rndKey))
          removalResult.isSuccess shouldBe true

          if (keyPosition > 0) {
            prover.performOneOperation(Remove(keys.head)).isSuccess shouldBe true
          }

          keys = keys.tail.filterNot(_.sameElements(rndKey))

          val shuffledKeys = scala.util.Random.shuffle(keys)
          shuffledKeys.foreach { k =>
            prover.performOneOperation(Remove(k)).isSuccess shouldBe true
          }
        }
        store.close()
        t.get
      }
    }
  }


  /**
    * All checks are being made with both underlying storage implementations
    * 1 LSMStore
    * 2 QuickStore
    */

  property("Persistence AVL batch prover (LSMStore backed) - rollback") {
    val prover = createPersistentProverWithLSM()
    rollbackTest(prover)
  }

  ignore("Persistence AVL batch prover (QuickStore backed) - rollback") {
    val prover = createPersistentProverWithQuick()
    rollbackTest(prover)
  }


  property("Persistence AVL batch prover (LSMStore backed) - basic test") {
    val store = createLSMStore()
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    basicTest(prover, storage)
  }

  ignore("Persistence AVL batch prover (QuickStore backed) - basic test") {
    val store = createQuickStore()
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    basicTest(prover, storage)
  }

  property("Persistence AVL batch prover (LSMStore backed) - rollback version") {
    val store = createLSMStore(1000)
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    rollbackVersionsTest(prover, storage)
  }

  ignore("Persistence AVL batch prover (QuickStore backed) - rollback version") {
    val store = createQuickStore(1000)
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    basicTest(prover, storage)
  }

  property("Persistence AVL batch prover (LSM backed) - remove single random element from a large set") {
    removeFromLargerSetSingleRandomElementTest(createLSMStore _)
  }

  ignore("Persistence AVL batch prover (QuickStore backed) - remove single random element from a large set") {
    removeFromLargerSetSingleRandomElementTest(createQuickStore _)
  }

}
