package scorex.crypto.authds.avltree.batch.benchmark.common

import scorex.crypto.authds.{ADKey, ADValue}
import scorex.crypto.authds.avltree.batch.{Insert, Operation, Update}
import scorex.crypto.authds.avltree.batch.benchmark.common.PersistentProverInitializer.Config
import scorex.utils.Random

trait OperationsOps {

  implicit class IntToOps(r: Range) {
    import com.google.common.primitives.Longs

    def toOps(implicit cfg: Config): Array[Operation] = {
      val insertsCount = r.length / 2

      val inserts = (r.head until r.head + insertsCount).map { i =>
        val key = ADKey @@  new Array[Byte](cfg.kl)
        val k = Longs.toByteArray(i) ++ Longs.toByteArray(System.currentTimeMillis)
        k.copyToArray(key)
        Insert(key, ADValue @@ k.take(cfg.vl))
      }.toArray

      val updates = inserts.map(i => Update(i.key, ADValue @@ Random.randomBytes(8)))

      inserts ++ updates
    }
  }
}
