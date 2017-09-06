package scorex.crypto.authds.avltree.batch.common

import com.google.common.primitives.Longs
import io.iohk.iodb.{FileAccess, LSMStore}
import scorex.crypto.authds.avltree.batch._
import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.hash.{Blake2b256Unsafe, Digest32}

object PersistentProverInitializer {

  def getPersistentProverWithLSMStore(keepVersions: Int, baseOperationsCount: Int)(implicit cfg: Config) = {
    val persProver = getPersProverWithLSMStore(keepVersions)

    if (baseOperationsCount > 0) {
      val step = 5000
      Range(0, baseOperationsCount, step).foreach { v =>
        val end = if (v + step == baseOperationsCount) {
          v + step + 1
        } else v + step
        (v until end).foreach { i =>
          val key = ADKey @@ new Array[Byte](cfg.kl)
          val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
          k.copyToArray(key)
          persProver.performOneOperation(Insert(key, ADValue @@ k.take(cfg.vl)))
        }
        persProver.generateProof()
      }
      persProver
    } else {
      persProver
    }
  }

  private def getPersProverWithLSMStore(keepVersions: Int)(implicit cfg: Config) = {
    implicit val hf = new Blake2b256Unsafe
    val dir = java.nio.file.Files.createTempDirectory("bench_testing_" + scala.util.Random.alphanumeric.take(15)).toFile
    dir.deleteOnExit()
    val store = new LSMStore(dir, keepVersions = keepVersions, fileAccess = FileAccess.UNSAFE)
    val storage = new VersionedIODBAVLStorage(store, NodeParameters(cfg.kl, cfg.vl, cfg.ll))
    require(storage.isEmpty)
    val prover = new BatchAVLProver[Digest32, Blake2b256Unsafe](cfg.kl, Some(cfg.vl))
    PersistentBatchAVLProver.create(prover, storage, paranoidChecks = true).get
  }

  case class Config(kl: Int, vl: Int, ll: Int)
}
