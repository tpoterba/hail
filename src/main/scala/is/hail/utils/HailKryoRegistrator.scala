package is.hail.utils

import com.esotericsoftware.kryo.Kryo
import is.hail.expr._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

class HailKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SerializableHadoopConfiguration])
    kryo.register(classOf[is.hail.annotations.UnsafeRow])
    kryo.register(classOf[Row])
    kryo.register(classOf[GenericRow])

    kryo.register(classOf[Type])
    kryo.register(classOf[TStruct])
    kryo.register(classOf[Field])
    kryo.register(classOf[Map[String, String]])

    kryo.register(classOf[Array[Field]])
    kryo.register(Class.forName("[I"))
  }
}
