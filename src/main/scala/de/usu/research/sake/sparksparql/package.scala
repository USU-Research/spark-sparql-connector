package de.usu.research.sake

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

package object sparksparql {

  /**
   * Adds a method, `sparqlSelect`, to SQLContext that allows querying sparql endpoint.
   */
  implicit class SparqlContext(sqlContext: SQLContext) extends Serializable {
    /**
     * Queries an SPARQL endpoint. All columns are returned as strings, if no userSchema is provided.
     * 
     * @param service SPARQL endpoint. E.g. "http://dbpedia.org/sparql"
     * @param query SPARQL query. E.g. <code>"select * where { ?s ?p ?o } limit 10"</code>
     * @param userSchema Optional parameter to explicitly set datatype for all columns.
     * <br>E.g. if the query <code>"SELECT ?flag ?count where { ... }"</code> should return boolean values for `flag`
     * and integer values for `count`, then set <pre><code>userSchema = StructType(List(StructField("flag", BooleanType, true), StructField("count", IntegerType, true)))</code></pre>   
     */
    def sparqlQuery(
        service: String,
        query: String,
        userSchema: StructType = null): DataFrame = {
      val sparqlRelation = SparqlRelation(
          service = service,
          query = query,
          userSchema = userSchema)(sqlContext)
      sqlContext.baseRelationToDataFrame(sparqlRelation)
    }
  }

}