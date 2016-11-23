package io.iohk.avliodb

import java.io.File

import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.hash.Blake2b256

import scala.collection.mutable.ArrayBuffer

class IODBStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {


  val dirname = "/tmp/iohk/avliodbtest"
  new File(dirname).mkdirs()
  new File(dirname).listFiles().foreach(f => f.delete())
  val store = new LSMStore(new File(dirname))

  property("IODB") {
    var version = store.lastVersion
    val keys: ArrayBuffer[(ByteArrayWrapper, ByteArrayWrapper)] = ArrayBuffer()
    forAll { b: Array[Byte] =>
      val pair = (ByteArrayWrapper(Blake2b256(0.toByte +: version.toByte +: b)),
        ByteArrayWrapper(Blake2b256(version.toByte +: b)))
      keys += pair
      store.update(version + 1, Seq(), Seq(pair))

      store.rollback(version)
      store.update(version + 1, Seq(), Seq(pair))
      version = version + 1
      keys.foreach(k => store.get(k._1).data shouldEqual k._2.data)

    }
  }

}
