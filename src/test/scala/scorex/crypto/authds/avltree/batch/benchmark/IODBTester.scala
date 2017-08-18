package scorex.crypto.authds.avltree.batch.benchmark

import java.io.File
import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import scorex.utils.Random

object IODBTester extends App {
  val Dirname = "/tmp/iohk/iodbfailer"
  val KL = 32
  val VL = 8
  val LL = 32
  val NumMods = 2000000
  val Step = 1000

  new File(Dirname).mkdirs()
  new File(Dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(Dirname))
  var currentVersion: Option[Long] = None

  val mods = generateModifications()

  (0 until(NumMods, Step)) foreach { i =>
    println(i)
    val mod: Seq[(ByteArrayWrapper, ByteArrayWrapper)] = mods.slice(i, i + Step)
    val nextVersion = ByteArrayWrapper.fromLong(i)
    store.update(nextVersion, Seq(), mod)
    currentVersion.foreach(v => {
      store.rollback(ByteArrayWrapper.fromLong(v))
      store.update(nextVersion, Seq(), mod)
    })
    currentVersion = Some(i)

    mods.slice(0, i + Step).foreach { m =>
      store(m._1).data
    }
  }

  def generateModifications(): Array[(ByteArrayWrapper,ByteArrayWrapper)] = {
    val mods = new Array[(ByteArrayWrapper,ByteArrayWrapper)](NumMods)

    for (i <- 0 until NumMods) {
      mods(i) = (ByteArrayWrapper(Random.randomBytes(KL)), ByteArrayWrapper(Random.randomBytes(VL)))
    }
    mods
  }
}
