package scorex.crypto.authds.benchmarks

import com.google.common.primitives.Longs
import io.iohk.iodb.{FileAccess, LSMStore}
import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.authds.avltree.batch._
import scorex.crypto.hash.{Blake2b256Unsafe, Digest32}
import scorex.utils.Random

object Helper {

  type Prover = PersistentBatchAVLProver[Digest32, Blake2b256Unsafe]

  implicit val hf = new Blake2b256Unsafe

  val kl = 32
  val vl = 8
  val ll = 32

  def generateOps(r: Range): Array[Operation] = {
    val insertsCount = r.length / 2

    val inserts = (r.head until r.head + insertsCount).map { i =>
      val key = ADKey @@ new Array[Byte](kl)
      val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
      k.copyToArray(key)
      Insert(key, ADValue @@ k.take(vl))
    }.toArray

    val updates = inserts.map(i => Update(i.key, ADValue @@ Random.randomBytes(8)))

    inserts ++ updates
  }

  def getPersistentProverWithLSMStore(keepVersions: Int, baseOperationsCount: Int = 0): (Prover, LSMStore, VersionedIODBAVLStorage[Digest32]) = {
    val dir = java.nio.file.Files.createTempDirectory("bench_testing_" + scala.util.Random.alphanumeric.take(15)).toFile
    dir.deleteOnExit()
    val store = new LSMStore(dir, keepVersions = keepVersions, fileAccess = FileAccess.UNSAFE)
    val storage = new VersionedIODBAVLStorage(store, NodeParameters(kl, Some(vl), ll))
    require(storage.isEmpty)
    val prover = new BatchAVLProver[Digest32, Blake2b256Unsafe](kl, Some(vl))


    val persProver = PersistentBatchAVLProver.create(prover, storage, paranoidChecks = true).get

    if (baseOperationsCount > 0) {
      val step = 5000
      Range(0, baseOperationsCount, step).foreach { v =>
        val end = if (v + step == baseOperationsCount) {
          v + step + 1
        } else v + step
        (v until end).foreach { i =>
          val key = ADKey @@ new Array[Byte](kl)
          val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
          k.copyToArray(key)
          persProver.performOneOperation(Insert(key, ADValue @@ k.take(vl)))
        }
        persProver.generateProofAndUpdateStorage()
      }
      (persProver, store, storage)
    } else {
      (persProver, store, storage)
    }
  }

  def getProver(baseOperationsCount: Int = 0): BatchAVLProver[Digest32, Blake2b256Unsafe] = {
    val prover = new BatchAVLProver[Digest32, Blake2b256Unsafe](kl, Some(vl))
    if (baseOperationsCount > 0) {
      val step = 5000
      Range(0, baseOperationsCount, step).foreach { v =>
        val end = if (v + step == baseOperationsCount) {
          v + step + 1
        } else v + step
        (v until end).foreach { i =>
          val key = ADKey @@ new Array[Byte](kl)
          val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
          k.copyToArray(key)
          prover.performOneOperation(Insert(key, ADValue @@ k.take(vl)))
        }
        prover.generateProof()
      }
      prover
    } else {
      prover
    }
  }

}
