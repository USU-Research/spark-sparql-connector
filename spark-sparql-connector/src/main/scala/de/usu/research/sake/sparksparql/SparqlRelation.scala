package de.usu.research.sake.sparksparql

import java.io.IOException
import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import org.slf4j.LoggerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.jena.jdbc.remote.RemoteEndpointDriver
import java.sql.DriverManager
import scala.collection.mutable.ArrayBuffer
import java.sql.ResultSet
import java.lang.{Boolean => JBoolean, Double => JDouble}
import org.apache.jena.jdbc.mem.MemDriver

case class SparqlRelation (
    service: String,
    query: String,
    defaultGraph: String,
    userSchema: StructType)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {

  private val logger = LoggerFactory.getLogger(SparqlRelation.getClass)

  override val schema: StructType = inferSchema()

  override def buildScan: RDD[Row] = {
    val schemaFields = schema.fields
    
    def extractValue(field: StructField, rs: ResultSet): AnyRef = {
      val name = field.name
      field.dataType match {
        case StringType =>
          rs.getString(name)
        case IntegerType =>
          val v = rs.getInt(name)
          if (rs.wasNull()) null else int2Integer(v)
        case LongType =>
          val v = rs.getLong(name)
          if (rs.wasNull()) null else long2Long(v)
        case BooleanType =>
          val v = rs.getBoolean(name)
          if (rs.wasNull()) null else boolean2Boolean(v)
        case DoubleType =>
          val v = rs.getDouble(name)
          if (rs.wasNull()) null else double2Double(v)
        case FloatType =>
          val v = rs.getFloat(name)
          if (rs.wasNull()) null else float2Float(v)
        case ShortType =>
          val v = rs.getShort(name)
          if (rs.wasNull()) null else short2Short(v)
        case ByteType =>
          val v = rs.getByte(name)
          if (rs.wasNull()) null else byte2Byte(v)
        case DateType =>
          rs.getDate(name)
        case TimestampType =>
          rs.getTimestamp(name)
        case _ =>
          throw new RuntimeException(s"Field $name has unsupported type ${field.dataType.json}")
      }
    }
    
    val conn = if (service.startsWith("mem:")) {
      MemDriver.register()
      DriverManager.getConnection(s"jdbc:jena:$service")
    } else {
      RemoteEndpointDriver.register()
      DriverManager.getConnection(s"jdbc:jena:remote:query=$service")
    }
    
    try {
      val stmt = conn.createStatement()
      try {
        val rs = stmt.executeQuery(query)
        val rows = ArrayBuffer[Row]()
        while (rs.next()) {
          val values = schemaFields.map(extractValue(_, rs))
          rows += Row.fromSeq(values)
        }
        rs.close()
        sqlContext.sparkContext.makeRDD(rows)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }


  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      val varNames = SparqlInspector.findResultVariables(query)
      // By default fields are assumed to be StringType
      val schemaFields = varNames.map { fieldName =>
        StructField(fieldName, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }
}