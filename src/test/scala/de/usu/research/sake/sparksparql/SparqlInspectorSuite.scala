package de.usu.research.sake.sparksparql

import org.scalatest.FunSuite

class SparqlInspectorSuite extends FunSuite {
  test("select *") {
    val s = """PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | SELECT *
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | ?x rdfs:label ?label
      |   FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
      | }""".stripMargin
    val vars = SparqlInspector.findResultVariables(s)
    assert(vars === Vector("x","label"))
  }
  
  test("select") {
    val s = """PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
      | PREFIX  ns:  <http://example.org/ns#>
      | SELECT  ?title (?p*(1-?discount) AS ?price)
      | { ?x ns:price ?p .
      |   ?x dc:title ?title . 
      |   ?x ns:discount ?discount 
      | }""".stripMargin
    val vars = SparqlInspector.findResultVariables(s)
    assert(vars === Vector("title", "price"))
  }

  test("construct") {
    val s = """PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      | PREFIX site: <http://example.org/stats#>
      | CONSTRUCT { [] foaf:name ?name }
      | WHERE
      | { [] foaf:name ?name ;
      |      site:hits ?hits .
      | }
      | ORDER BY desc(?hits)
      | LIMIT 2""".stripMargin
    val vars = SparqlInspector.findResultVariables(s)
    assert(vars === Vector("Subject", "Predicate", "Object"))
  }
  
  test("ask") {
    val s = """PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | ASK
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | }""".stripMargin
    val vars = SparqlInspector.findResultVariables(s)
    assert(vars === Vector("ASK"))
  }

  test("describe") {
    val s = """PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | DESCRIBE ?x
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | }""".stripMargin
    val vars = SparqlInspector.findResultVariables(s)
    assert(vars === Vector("Subject", "Predicate", "Object"))
  }
}