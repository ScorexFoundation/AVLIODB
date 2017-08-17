package scorex.crypto.authds.avltree.batch

import java.io.File

import com.google.common.primitives.Ints
import io.iohk.iodb.LSMStore
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.encode.Base58
import scorex.crypto.hash.{Blake2b256, Blake2b256Unsafe}
import scorex.utils.Random

import scala.reflect.io.Path
import scala.util.{Failure, Try}

class VersionedIODBAVLStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {

  val KeyLength = 32
  val ValueLength = 8
  val LabelLength = 32

  implicit val hf = new Blake2b256Unsafe

  //todo: move to tests
  val dirname = "/tmp/iohk/avliodb"
  new File(dirname).mkdirs()
  new File(dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(dirname))
  val storage = new VersionedIODBAVLStorage(store, NodeParameters(KeyLength, ValueLength, LabelLength))
  require(storage.isEmpty)
  val prover = PersistentBatchAVLProver.create(new BatchAVLProver(KeyLength, Some(ValueLength), None), storage, true).get


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

    val prover2 = PersistentBatchAVLProver.create(new BatchAVLProver(KeyLength, Some(ValueLength), None), storage).get
    Base58.encode(prover2.digest) shouldBe Base58.encode(prover.digest)
    prover2.checkTree(postProof = true)
  }

  property("remove single random element from a large set") {

    val minSetSize = 10000
    val maxSetSize = 200000

    forAll(Gen.choose(minSetSize, maxSetSize), Arbitrary.arbBool.arbitrary) { case (cnt, generateProof) =>
      whenever(cnt > minSetSize) {

        val suffixBytes = hf(System.currentTimeMillis() + " : " + new String(Random.randomBytes(20))).take(4)
        val suffixInt = Ints.fromByteArray(suffixBytes)

        val dirName = "/tmp/iohk/avliodb"+suffixInt

        val dir = new File(dirName)
        dir.mkdirs().ensuring(_ == true)
        //dir.listFiles().foreach(f => f.delete())
        val store = new LSMStore(new File(dirName)).ensuring(_.lastVersionID.isEmpty)

        val t = Try {

          var keys = IndexedSeq[Array[Byte]]()
          val p = new BatchAVLProver(KeyLength, Some(ValueLength))

          (1 to cnt) foreach { _ =>
            val key = Random.randomBytes(KeyLength)
            val value = Random.randomBytes(ValueLength)

            keys = key +: keys

            p.performOneOperation(Insert(key, value)).isSuccess shouldBe true
            p.unauthenticatedLookup(key).isDefined shouldBe true
          }

          if (generateProof) p.generateProof()

          val storage = new VersionedIODBAVLStorage(store, NodeParameters(KeyLength, ValueLength, LabelLength))
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
