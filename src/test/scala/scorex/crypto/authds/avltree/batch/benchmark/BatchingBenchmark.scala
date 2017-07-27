package scorex.crypto.authds.avltree.batch.benchmark

import java.io.File

import io.iohk.iodb.LSMStore
import scorex.crypto.authds.avltree.batch.{VersionedIODBAVLStorage, _}
import scorex.crypto.hash.Blake2b256Unsafe
import scorex.utils.Random

object BatchingBenchmark extends App {
  val Dirname = "/tmp/iohk/avliodbbench"
  val KeyLength = 26
  val ValueLength = 8
  val LabelLength = 32
  val InitialMods = 0
  val NumMods = 2000000


  implicit val hf = new Blake2b256Unsafe
  new File(Dirname).mkdirs()
  new File(Dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(Dirname), keepVersions = 100)
  val storage = new VersionedIODBAVLStorage(store, KeyLength, ValueLength, LabelLength)
  require(storage.isEmpty)


  var digest = Array[Byte]()

  var numInserts = 0

  val mods = generateModifications()

  println(s"NumInserts = $numInserts")
  println("Step, In-memory prover time, Persistent prover time, Rollback time")

  bench()

  def bench(): Unit = {
    val prover = new BatchAVLProver(KeyLength, Some(ValueLength), None)
    val persProver = new PersistentBatchAVLProver(new BatchAVLProver(KeyLength, Some(ValueLength), None), storage)

    val Step = 2000
    digest = persProver.digest
    require(persProver.digest sameElements prover.digest)
    (0 until(NumMods, Step)) foreach { i =>
      oneStep(i, Step, i / 2, persProver, prover)
    }
  }


  def oneStep(i: Int, step: Int, toPrint: Int, persProver: PersistentBatchAVLProver[_], prover: BatchAVLProver[_]): Unit = {
    System.gc()
    val converted = mods.slice(i, i + step)

    val (persProverTime, _) = time {
      converted.foreach(c => persProver.performOneOperation(c))
      persProver.digest
      persProver.generateProof
    }
    val (rollbackTime, _) = time {
      persProver.rollback(digest).get
      persProver.digest
    }
    converted.foreach(c => persProver.performOneOperation(c))
    persProver.generateProof

    val (proverTime, _) = time {
      converted.foreach(c => prover.performOneOperation(c))
      prover.generateProof
      prover.digest
    }

    digest = persProver.digest
    assert(prover.digest sameElements digest)

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
      if (i == 0 || i < InitialMods || i % 2 == 0) {
        // with prob ~.5 insert a new one, with prob ~.5 update an existing one
        mods(i) = Insert(Random.randomBytes(KeyLength), Random.randomBytes(8))
        numInserts += 1
      } else {
        val j = Random.randomBytes(3)
        mods(i) = Update(mods((j(0).toInt.abs + j(1).toInt.abs * 128 + j(2).toInt.abs * 128 * 128) % i).key, Random.randomBytes(8))
      }
    }
    mods
  }


}
