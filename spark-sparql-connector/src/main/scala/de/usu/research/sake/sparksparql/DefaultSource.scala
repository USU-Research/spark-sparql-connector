package de.usu.research.sake.sparksparql

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {
  override def shortName(): String = "sparql"

  /**
   * Creates a new relation for data store in SPARQL given parameters.
   * Parameters have to include 'service' and 'query'
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): SparqlRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in SPARQL given parameters and user supported schema.
   * Parameters have to include 'service' and 'query'
   */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): SparqlRelation = {
    val service = checkService(parameters)
    val query = checkQuery(parameters)
    SparqlRelation(service, query, schema)(sqlContext)
  }

  private def checkService(parameters: Map[String, String]): String = {
    parameters.getOrElse("service", sys.error("'service' must be specified for SPARQL data."))
  }

  private def checkQuery(parameters: Map[String, String]): String = {
    parameters.getOrElse("query", sys.error("'query' must be specified for SPARQL data."))
  }
}