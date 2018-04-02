package scorex.crypto.authds.avltree.batch

import com.google.common.primitives.Longs
import io.iohk.iodb.{ByteArrayWrapper, Store}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.authds.avltree.batch.helpers.TestHelper
import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.encode.Base58
import scorex.crypto.hash.{Blake2b256, Blake2b256Unsafe, Digest32}
import scorex.utils.{Random => RandomBytes}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Random, Success, Try}

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

    def ops(s: Int, e: Int): Unit = (s until e).foreach { i =>
      prover.performOneOperation(Insert(ADKey @@ Blake2b256("k" + i).take(KL),
        ADValue @@ Blake2b256("v" + i).take(VL)))
    }

    ops(0, 100)
    prover.generateProofAndUpdateStorage()

    val digest = prover.digest
    val digest58String = digest.toBase58

    ops(100, 200)
    prover.generateProofAndUpdateStorage()

    prover.digest.toBase58 should not equal digest58String

    prover.rollback(digest)

    prover.digest.toBase58 shouldEqual digest58String

    prover.checkTree(true)
  }

  // Read-only access to some elements in parallel should not affect modifications application
  val parallelReadTest: PERSISTENT_PROVER => Unit = { prover: PERSISTENT_PROVER =>
    val pairs = (1 to 10000).map(_ => kvGen.sample.get)
    pairs.foreach(p => prover.performOneOperation(Insert(p._1, p._2)))
    prover.generateProofAndUpdateStorage

    forAll(Gen.choose(1, 10), Gen.choose(1, 10), Arbitrary.arbitrary[Long]) { (numRemoves, numInserts, seed) =>
      val inserts: Seq[Insert] = (0 until numInserts).flatMap(_ => kvGen.sample).map(kv => Insert(kv._1, kv._2))
      val startRoot = prover.digest

      // Parallel access to prover.digest should not lead to application failure
      Future {
        Try {
          (0 until 1000) map { _ =>
            Thread.sleep(10)
            prover.digest
          }
        }
      }

      inserts.foldLeft[Try[Option[ADValue]]](Success(None)) { case (t, m) =>
        t.flatMap(_ => prover.performOneOperation(m))
      }.get
      prover.generateProofAndUpdateStorage()
      prover.rollback(startRoot)
      prover.digest shouldEqual startRoot
    }

  }


  // Test similar to blockchain workflow - generate proofs for some modifications, rollback, apply modifications
  val blockchainWorkflowTest: PERSISTENT_PROVER => Unit = { prover: PERSISTENT_PROVER =>

    val pairs = (1 to 10000).map(_ => kvGen.sample.get)
    pairs.foreach(p => prover.performOneOperation(Insert(p._1, p._2)))
    prover.generateProofAndUpdateStorage

    forAll(Gen.choose(1, 10), Gen.choose(1, 10), Arbitrary.arbitrary[Long]) { (numRemoves, numInserts, seed) =>
      val rnd = new Random(seed)
      val startRoot = prover.digest

      val inserts: Seq[Insert] = (0 until numInserts).flatMap(_ => kvGen.sample).map(kv => Insert(kv._1, kv._2))
      val removes: Seq[Remove] = (0 until numRemoves).flatMap(i => prover.avlProver.randomWalk(rnd))
        .groupBy(a => Base58.encode(a._1)).map(_._2.head).map(kv => Remove(kv._1)).toSeq
      val mods = removes ++ inserts

      mods.foldLeft[Try[Option[ADValue]]](Success(None)) { case (t, m) =>
        t.flatMap(_ => prover.performOneOperation(m))
      }.get
      prover.generateProofAndUpdateStorage()
      prover.digest

      prover.rollback(startRoot).get
      prover.digest shouldEqual startRoot

      mods.foldLeft[Try[Option[ADValue]]](Success(None)) { case (t, m) =>
        t.flatMap(_ => prover.performOneOperation(m))
      }.get

      prover.generateProofAndUpdateStorage()
      prover.digest
    }
  }

  val basicTest: (PERSISTENT_PROVER, STORAGE) => Unit = { (prover: PERSISTENT_PROVER, storage: STORAGE) =>
    var digest = prover.digest

    def oneMod(aKey: ADKey, aValue: ADValue): Unit = {
      prover.digest shouldBe digest

      val m = Insert(aKey, aValue)
      prover.performOneOperation(m)
      val pf = prover.generateProofAndUpdateStorage()

      val verifier = createVerifier(digest, pf)
      verifier.performOneOperation(m).isSuccess shouldBe true
      prover.digest.toBase58 should not equal digest.toBase58
      prover.digest.toBase58 shouldEqual verifier.digest.get.toBase58

      prover.rollback(digest).get

      prover.checkTree(true)

      prover.digest shouldBe digest

      prover.performOneOperation(m)
      val pf2 = prover.generateProofAndUpdateStorage()

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
      prover.generateProofAndUpdateStorage()
      prover.digest
    }
    noException should be thrownBy storage.rollbackVersions.foreach(v => prover.rollback(v).get)
  }

  def testAddInfoSaving(createStore: (Int) => Store): Unit = {
    val store = createStore(1000)
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)

    implicit def arrayToWrapper(in: Array[Byte]): ByteArrayWrapper = ByteArrayWrapper(in)

    val insert1 = Insert(ADKey @@ RandomBytes.randomBytes(32), ADValue @@ Longs.toByteArray(1L))
    val insert2 = Insert(ADKey @@ RandomBytes.randomBytes(32), ADValue @@ Longs.toByteArray(2L))
    val insert3 = Insert(ADKey @@ RandomBytes.randomBytes(32), ADValue @@ Longs.toByteArray(3L))
    val insert4 = Insert(ADKey @@ RandomBytes.randomBytes(32), ADValue @@ Longs.toByteArray(4L))
    val insert5 = Insert(ADKey @@ RandomBytes.randomBytes(32), ADValue @@ Longs.toByteArray(5L))

    val addInfo1 = RandomBytes.randomBytes(32) -> Longs.toByteArray(6L)
    val addInfo2 = RandomBytes.randomBytes(32) -> Longs.toByteArray(7L)

    prover.performOneOperation(insert1)
    prover.generateProofAndUpdateStorage()
    val digest1 = prover.digest

    prover.performOneOperation(insert2)
    prover.performOneOperation(insert3)
    prover.generateProofAndUpdateStorage(Seq(addInfo1))

    val digest2 = prover.digest

    prover.performOneOperation(insert4)
    prover.performOneOperation(insert5)
    prover.generateProofAndUpdateStorage(Seq(addInfo2))


    store.get(addInfo1._1) shouldBe defined
    store.get(addInfo2._1) shouldBe defined

    storage.rollback(digest2).get

    store.get(addInfo1._1) shouldBe defined
    store.get(addInfo2._1) shouldBe None

    storage.rollback(digest1).get

    store.get(addInfo1._1) shouldBe None
    store.get(addInfo2._1) shouldBe None

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

  property("Persistence AVL batch prover (LSMStore backed) - parallel read-write") {
    val prover = createPersistentProverWithLSM()
    parallelReadTest(prover)
  }


  property("Persistence AVL batch prover (LSMStore backed) - blockchain workflow") {
    val prover = createPersistentProverWithLSM()
    blockchainWorkflowTest(prover)
  }

  property("Persistence AVL batch prover (QuickStore backed) - blockchain workflow") {
    val prover = createPersistentProverWithQuick()
    blockchainWorkflowTest(prover)
  }

  property("Persistence AVL batch prover (LSMStore backed) - rollback") {
    val prover = createPersistentProverWithLSM()
    rollbackTest(prover)
  }

  property("Persistence AVL batch prover (QuickStore backed) - rollback") {
    val prover = createPersistentProverWithQuick()
    rollbackTest(prover)
  }


  property("Persistence AVL batch prover (LSMStore backed) - basic test") {
    val store = createLSMStore()
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    basicTest(prover, storage)
  }

  property("Persistence AVL batch prover (QuickStore backed) - basic test") {
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

  property("Persistence AVL batch prover (QuickStore backed) - rollback version") {
    val store = createQuickStore(1000)
    val storage = createVersionedStorage(store)
    val prover = createPersistentProver(storage)
    basicTest(prover, storage)
  }

  property("Persistence AVL batch prover (LSM backed) - remove single random element from a large set") {
    removeFromLargerSetSingleRandomElementTest(createLSMStore _)
  }

  property("Persistence AVL batch prover (QuickStore backed) - remove single random element from a large set") {
    removeFromLargerSetSingleRandomElementTest(createQuickStore _)
  }

  property("Persistence AVL batch prover (LSM backed) - save additional info") {
    testAddInfoSaving(createLSMStore _)
  }

  property("Persistence AVL batch prover (Quick Store backed) - save additional info") {
    testAddInfoSaving(createQuickStore _)
  }

}
