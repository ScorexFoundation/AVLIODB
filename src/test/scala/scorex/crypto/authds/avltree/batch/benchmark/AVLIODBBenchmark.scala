package scorex.crypto.authds.avltree.batch.benchmark

import com.google.common.primitives.Longs
import io.iohk.iodb.{FileAccess, LSMStore}
import org.scalameter.api._
import scorex.crypto.authds.avltree.batch._
import scorex.crypto.hash.Blake2b256Unsafe
import scorex.utils.{Random, ScryptoLogging}


object AVLIODBBenchmark extends Bench.ForkedTime with ScryptoLogging {

  private val max = 10000
  private val step = 2000

  private val preHeatedOperationsSize = 50000000
  private val storeKeepVersions = 0

  val sizes = Gen.range(s"$step operations from")(preHeatedOperationsSize + 1, preHeatedOperationsSize + max, step)
  val mods = for (ord <- sizes) yield ord until ord + step

  val KL = 32
  val VL = 8
  val LL = 32

  val persProver = getPersistentProverWithTree(preHeatedOperationsSize)

  performance of "AVLIODBBatchExecution" in {
    measure method s"apply operations to pers prover with $preHeatedOperationsSize performed operations" in {
      using(mods) config(
        exec.benchRuns -> 2,
        exec.maxWarmupRuns -> 2,
        exec.minWarmupRuns -> 2
      ) in { modifications =>
        modifications.toOps.foreach(persProver.performOneOperation)
        persProver.digest
        persProver.generateProof()
      }
    }
  }

  private def getPersistentProver = {
    implicit val hf = new Blake2b256Unsafe
    val dir = java.nio.file.Files.createTempDirectory("bench_testing_" + scala.util.Random.alphanumeric.take(15)).toFile
    dir.deleteOnExit()
    val store = new LSMStore(dir, keepVersions = storeKeepVersions, fileAccess = FileAccess.UNSAFE)
    val storage = new VersionedIODBAVLStorage(store, NodeParameters(KL, VL, LL))
    require(storage.isEmpty)
    val prover = new BatchAVLProver(KL, Some(VL))
    PersistentBatchAVLProver.create(prover, storage, paranoidChecks = true).get
  }

  def getPersistentProverWithTree(baseOperationsCount: Int = preHeatedOperationsSize) = {
    val persProver = getPersistentProver

    if (baseOperationsCount > 0 ) {
      val step = 5000
      Range(0, baseOperationsCount, step).foreach { v =>
        val end = if (v + step == baseOperationsCount) { v + step + 1} else v + step
        (v until end).foreach { i =>
          val key = new Array[Byte](KL)
          val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
          k.copyToArray(key)
          persProver.performOneOperation(Insert(key, k.take(VL)))
        }
        persProver.generateProof()
      }
      persProver
    } else {
      persProver
    }
  }

  implicit class IntToOps(r: Range) {
    import com.google.common.primitives.Longs

    def toOps: Array[Operation] = {
      val inserts = r.map { i =>
        val key = new Array[Byte](KL)
        val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
        k.copyToArray(key)
        Insert(key, k.take(VL))
      }.toArray

      val updates = inserts.map(i => Update(i.key, Random.randomBytes(8)))

      inserts ++ updates
    }
  }


}
