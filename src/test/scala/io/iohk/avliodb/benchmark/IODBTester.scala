package io.iohk.avliodb.benchmark

import java.io.File

import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import scorex.crypto.authds.avltree.batch._
import scorex.utils.Random

object IODBTester extends App {
  val Dirname = "/tmp/iohk/iodbfailer"
  val KL = 32
  val VL = 8
  val LL = 32
  val InitilaMods = 0
  val NumMods = 2000000
  val Step = 1000


  new File(Dirname).mkdirs()
  new File(Dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(Dirname))
  var version = store.lastVersion


  val mods = generateModifications()

  (0 until(NumMods, Step)) foreach { i =>
    println(i)
    val mod: Seq[(ByteArrayWrapper, ByteArrayWrapper)] = mods.slice(i, i + Step)
    store.update(version + 1, Seq(), mod)
    store.rollback(version)
    store.update(version + 1, Seq(), mod)
    version = store.lastVersion

    mods.slice(0, i + Step).foreach { m =>
      store.get(m._1).data
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
