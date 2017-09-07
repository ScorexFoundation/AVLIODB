package scorex.crypto.authds.avltree.batch

import java.io.File

import com.google.common.primitives.Ints
import io.iohk.iodb.LSMStore
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.encode.Base58
import scorex.crypto.hash.{Blake2b256, Blake2b256Unsafe, Digest32}
import scorex.utils.{Random => RandomBytes}

import scala.reflect.io.Path
import scala.util.{Random, Success, Try}

class VersionedIODBAVLStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {

  val KL = 32
  val VL = 8
  val LL = 32

  implicit val hf = new Blake2b256Unsafe

  def withProver[A](action: (PersistentBatchAVLProver[Digest32, Blake2b256Unsafe],
    VersionedIODBAVLStorage[Digest32]) => A): A = withProver[A]()(action)

  def withProver[A](keepVersions: Int = 0)
                   (action: (PersistentBatchAVLProver[Digest32, Blake2b256Unsafe],
                     VersionedIODBAVLStorage[Digest32]) => A): A = {
    val dir = getRandomTempDir
    val store = new LSMStore(dir, keepVersions = keepVersions)
    val storage = new VersionedIODBAVLStorage(store, NodeParameters(KL, VL, LL))
    require(storage.isEmpty)
    val Success(prover) = PersistentBatchAVLProver.create[Digest32, Blake2b256Unsafe](
      new BatchAVLProver[Digest32, Blake2b256Unsafe](KL, Some(VL)), storage, paranoidChecks = true)

    val res = action(prover, storage)

    Path(dir).deleteRecursively()
    res
  }

  private def getRandomTempDir: File = {
    val dir = java.nio.file.Files.createTempDirectory("avliodb_test_" + Random.alphanumeric.take(15)).toFile
    dir.deleteOnExit()
    dir
  }

  def kvGen: Gen[(ADKey, ADValue)] = for {
    key <- Gen.listOfN(KL, Arbitrary.arbitrary[Byte]).map(_.toArray) suchThat
      (k => !(k sameElements Array.fill(KL)(-1: Byte)) && !(k sameElements Array.fill(KL)(0: Byte)) && k.length == KL)
    value <- Gen.listOfN(VL, Arbitrary.arbitrary[Byte]).map(_.toArray)
  } yield (ADKey @@ key, ADValue @@ value)

  property("Persistence AVL batch prover - rollback") {
    withProver { (prover, _) =>
      (0 until 100) foreach { i =>
        prover.performOneOperation(Insert(ADKey @@ Blake2b256("k" + i).take(KL),
          ADValue @@ Blake2b256("v" + i).take(VL)))
      }
      prover.generateProofAndUpdateStorage()

      val digest = prover.digest
      (100 until 200) foreach { i =>
        prover.performOneOperation(Insert(ADKey @@ Blake2b256("k" + i).take(KL),
          ADValue @@ Blake2b256("v" + i).take(VL)))
      }
      prover.generateProofAndUpdateStorage()
      Base58.encode(prover.digest) should not equal Base58.encode(digest)

      prover.rollback(digest).get
      Base58.encode(prover.digest) shouldEqual Base58.encode(digest)

      prover.checkTree(true)
    }
  }


  property("Persistence AVL batch prover - basic test") {

    withProver { (prover, storage) =>
      var digest = prover.digest

      def oneMod(aKey: ADKey, aValue: ADValue): Unit = {
        prover.digest shouldBe digest

        val m = Insert(aKey, aValue)
        prover.performOneOperation(m)
        val pf = prover.generateProofAndUpdateStorage()

        val verifier = new BatchAVLVerifier[Digest32, Blake2b256Unsafe](digest, pf, KL, Some(VL))
        verifier.performOneOperation(m).isSuccess shouldBe true
        Base58.encode(prover.digest) should not equal Base58.encode(digest)
        Base58.encode(prover.digest) shouldEqual Base58.encode(verifier.digest.get)

        prover.rollback(digest).get

        prover.checkTree(true)

        prover.digest shouldBe digest

        prover.performOneOperation(m)
        val pf2 = prover.generateProofAndUpdateStorage()

        pf shouldBe pf2

        prover.checkTree(true)

        val verifier2 = new BatchAVLVerifier[Digest32, Blake2b256Unsafe](digest, pf2, KL, Some(VL))
        verifier2.performOneOperation(m).isSuccess shouldBe true

        digest = prover.digest
      }

      (1 to 100).foreach { _ =>
        val (aKey, aValue) = kvGen.sample.get
        oneMod(aKey, aValue)
      }

      val prover2 = PersistentBatchAVLProver.create(new BatchAVLProver[Digest32, Blake2b256Unsafe](KL, Some(VL), None),
        storage).get
      Base58.encode(prover2.digest) shouldBe Base58.encode(prover.digest)
      prover2.checkTree(postProof = true)
    }
  }

  property("Persistence AVL batch prover - rollback version") {
    withProver(10) { (prover, storage) =>
      (0L until 50L).foreach { long =>
        val insert = Insert(ADKey @@ RandomBytes.randomBytes(32),
          ADValue @@ com.google.common.primitives.Longs.toByteArray(long))
        prover.performOneOperation(insert)
        prover.generateProofAndUpdateStorage()
        prover.digest
      }
      noException should be thrownBy storage.rollbackVersions.foreach(v => prover.rollback(v).get)
    }
  }

  property("Persistence AVL batch prover - remove single random element from a large set") {

    val minSetSize = 10000
    val maxSetSize = 200000

    forAll(Gen.choose(minSetSize, maxSetSize), Arbitrary.arbBool.arbitrary) { case (cnt, generateProof) =>
      whenever(cnt > minSetSize) {

        val suffixBytes = hf(System.currentTimeMillis() + " : " + new String(RandomBytes.randomBytes(20))).take(4)
        val suffixInt = Ints.fromByteArray(suffixBytes)

        val dirName = "/tmp/iohk/avliodb" + suffixInt

        val dir = new File(dirName)
        dir.mkdirs().ensuring(_ == true)
        //dir.listFiles().foreach(f => f.delete())
        val store = new LSMStore(new File(dirName)).ensuring(_.lastVersionID.isEmpty)

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

          val storage = new VersionedIODBAVLStorage(store, NodeParameters(KL, VL, LL))
          assert(storage.isEmpty)

          val prover = PersistentBatchAVLProver.create(p, storage, true).get

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
        Path(dir).deleteRecursively()
        t.get
      }
    }
  }

}
