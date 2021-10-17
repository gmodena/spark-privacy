
package org.apache.spark

import com.google.privacy.differentialprivacy.Count
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, DoubleType, IntegerType, LongType, StructField, StructType, UserDefinedType}

class PrivateCountUDT extends UserDefinedType[Count] {
    override def sqlType: DataType = StructType(
        StructField("countSummary", ArrayType(ByteType), false) ::
          Nil)


    override def serialize(obj: Count): Any = {
        val row = InternalRow(1)
        row.update(0, UnsafeArrayData.fromPrimitiveArray(obj.getSerializableSummary))
    }

    override def deserialize(datum: Any): Count = datum match {
        case row: InternalRow =>
            require(row.numFields == 1, s"expected row length 1, got ${row.numFields}")
            val countSummary = row.getArray(0).toByteArray()
            Count.builder().build() // TODO(gmodena) we need to build Count from a s
        case u => throw new Exception(s"failed to deserialize: $u")
    }

    override def userClass: Class[Count] = classOf[Count]
}
