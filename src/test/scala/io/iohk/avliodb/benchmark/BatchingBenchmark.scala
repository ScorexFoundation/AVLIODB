package io.iohk.avliodb.benchmark

import java.io.File

import io.iohk.avliodb.VersionedIODBAVLStorage
import io.iohk.iodb.LSMStore
import scorex.crypto.authds.avltree.batch._
import scorex.crypto.hash.Blake2b256Unsafe
import scorex.utils.Random

object BatchingBenchmark extends App {
  val Dirname = "/tmp/iohk/avliodbbench"
  val KL = 26
  val VL = 8
  val LL = 32
  val InitilaMods = 0
  val NumMods = 2000000


  implicit val hf = new Blake2b256Unsafe
  new File(Dirname).mkdirs()
  new File(Dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(Dirname))
  val storage = new VersionedIODBAVLStorage(store, KL, VL, LL)
  require(storage.isEmpty)


  var digest = Array[Byte]()

  var numInserts = 0

  val mods = generateModifications()

  println(s"NumInserts = $numInserts")
  println("Step, In-memory prover time, Persistent prover time, Rollback time")

  bench()

  def bench(): Unit = {
    val prover = new BatchAVLProver(None, KL, VL)
    val persProver = new PersistentBatchAVLProver(new BatchAVLProver(None, KL, VL), storage)

    val Step = 2000
    digest = persProver.rootHash
    require(persProver.rootHash sameElements prover.rootHash)
    (0 until(NumMods, Step)) foreach { i =>
      oneStep(i, Step, i / 2, persProver, prover)
    }
  }


  def oneStep(i: Int, step: Int, toPrint: Int, persProver: PersistentBatchAVLProver[_], prover: BatchAVLProver[_]): Unit = {
    System.gc()
    val converted = mods.slice(i, i + step)

    val (persProverTime, _) = time {
      converted.foreach(c => persProver.performOneModification(c))
      persProver.rootHash
      persProver.generateProof
    }
    val (rollbackTime, _) = time {
      persProver.rollback(digest).get
      persProver.rootHash
    }
    converted.foreach(c => persProver.performOneModification(c))
    persProver.generateProof

    val (proverTime, _) = time {
      converted.foreach(c => prover.performOneModification(c))
      prover.generateProof
      prover.rootHash
    }

    digest = persProver.rootHash
    assert(prover.rootHash sameElements digest)

    println(s"$toPrint,$proverTime,$persProverTime,$rollbackTime")
  }

  def time[R](block: => R): (Float, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    ((t1 - t0).toFloat / 1000000, result)
  }

  def generateModifications(): Array[Modification] = {
    val mods = new Array[Modification](NumMods)

    for (i <- 0 until NumMods) {
      if (i == 0 || i < InitilaMods || i % 2 == 0) {
        // with prob ~.5 insert a new one, with prob ~.5 update an existing one
        mods(i) = Insert(Random.randomBytes(KL), Random.randomBytes(8))
        numInserts += 1
      } else {
        val j = Random.randomBytes(3)
        mods(i) = Update(mods((j(0).toInt.abs + j(1).toInt.abs * 128 + j(2).toInt.abs * 128 * 128) % i).key, Random.randomBytes(8))
      }
    }
    mods
  }


}
