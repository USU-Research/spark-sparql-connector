package de.usu.research.sake

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

package object sparksparql {

  /**
   * Adds a method, `sparqlSelect`, to SQLContext that allows querying sparql endpoint.
   */
  implicit class SparqlContext(sqlContext: SQLContext) extends Serializable {
    def sparqlSelect(
        service: String,
        query: String,
        defaultGraph: String = null,
        userSchema: StructType = null): DataFrame = {
      val sparqlRelation = SparqlRelation(
          service = service,
          query = query,
          defaultGraph = defaultGraph,
          userSchema = userSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }

}