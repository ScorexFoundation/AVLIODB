package scorex.crypto.authds.avltree.batch

import java.nio.file.Files

import com.google.common.primitives.Longs
import io.iohk.iodb.{ByteArrayWrapper, LSMStore}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.crypto.hash.Blake2b256

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class IODBStorageSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {

  property("IODB with LSM") {
    val dir = Files.createTempDirectory("iodb_lsm_" + Random.alphanumeric.take(10).mkString).toFile
    dir.deleteOnExit()
    val store = new LSMStore(dir)
    var version = store.lastVersionID.map(v => Longs.fromByteArray(v.data))
    val keys: ArrayBuffer[(ByteArrayWrapper, ByteArrayWrapper)] = ArrayBuffer()
    forAll { b: Array[Byte] =>
      val pair = (ByteArrayWrapper(Blake2b256(0.toByte +: version.getOrElse(0L).toByte +: b)),
        ByteArrayWrapper(Blake2b256(version.getOrElse(0L).toByte +: b)))
      keys += pair
      val nextVersion = version.getOrElse(0L) + 1
      store.update(nextVersion, Seq(), Seq(pair))

      if (version.isDefined) {
        store.rollback(ByteArrayWrapper.fromLong(version.get))
        store.update(nextVersion, Seq(), Seq(pair))
      }
      version = Some(nextVersion)
      keys.foreach(k => store(k._1).data shouldEqual k._2.data)

    }
  }
}
