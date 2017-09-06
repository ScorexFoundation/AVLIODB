package scorex.crypto.authds.avltree.batch

import org.scalameter.api._
import scorex.crypto.authds.avltree.batch.common.{OperationsOps, PersistentProverInitializer}
import scorex.crypto.authds.avltree.batch.common.PersistentProverInitializer.Config


object Performance extends Bench.ForkedTime with OperationsOps {

  lazy val sizes = Gen.range(s"$step operations from")(operationsSize + 1, operationsSize + max, step)
  lazy val mods = for (ord <- sizes) yield ord until ord + step
  val KL = 32
  val VL = 8
  val LL = 32
  implicit val cfg = Config(KL, VL, LL)
  lazy val persProverWithLSM =
    PersistentProverInitializer.getPersistentProverWithLSMStore(keepVersions, operationsSize)
  private val max = 10000
  private val step = 2000
  private val operationsSize = 1000000
  private val keepVersions = 0

  performance of "AVLIODBBatchExecution" in {
    measure method s"apply operations to pers prover with $operationsSize performed operations (LSMStore)" in {
      using(mods) config(
        exec.benchRuns -> 2,
        exec.maxWarmupRuns -> 1,
        exec.minWarmupRuns -> 1
        ) in { modifications =>
        modifications.toOps.foreach(persProverWithLSM.performOneOperation)
        persProverWithLSM.digest
        persProverWithLSM.generateProof()
      }
    }
  }
}
