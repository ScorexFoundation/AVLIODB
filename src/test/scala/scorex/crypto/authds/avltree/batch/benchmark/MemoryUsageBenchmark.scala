package scorex.crypto.authds.avltree.batch.benchmark

import org.scalameter.api._
import scorex.crypto.authds.avltree.batch.benchmark.common.PersistentProverInitializer
import scorex.crypto.authds.avltree.batch.benchmark.common.PersistentProverInitializer.Config

object MemoryUsageBenchmark extends Bench.ForkedTime {

  val ops = Gen.range("operations in tree")(startSize, finishSize, step)
  val KL = 32
  val VL = 8
  val LL = 32
  implicit val cfg = Config(KL, VL, LL)
  private val keepVersions = 0
  private val startSize = 10000
  private val finishSize = 100000
  private val step = 10000

  override def measurer = new Measurer.MemoryFootprint

  performance of "AVLIODBBatchExecution" in {
    measure method s"memory footprint of prover tree with LSMStore" in {
      using(ops) config(
        exec.benchRuns -> 1,
        exec.maxWarmupRuns -> 2,
        exec.minWarmupRuns -> 2,
        exec.independentSamples -> 1,
        exec.outliers.retries -> 4
        ) in { operationsCount =>
        PersistentProverInitializer.getPersistentProverWithLSMStore(keepVersions, operationsCount)
      }
    }
  }
}
