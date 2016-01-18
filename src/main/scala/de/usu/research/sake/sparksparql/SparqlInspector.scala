package de.usu.research.sake.sparksparql

import org.apache.jena.query.QueryFactory
import scala.collection.JavaConversions._

object SparqlInspector {
  def findResultVariables(queryString: String): Vector[String] = {
    val query = QueryFactory.create(queryString)
    if (query.isSelectType()) {
      query.getResultVars.toVector
    } else if (query.isConstructType() || query.isDescribeType()) {
        Vector("Subject", "Predicate", "Object")    
    } else if (query.isAskType()) {
        Vector("ASK")    
    } else {
      query.getResultVars.toVector
    }
  }
}