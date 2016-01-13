package de.usu.research.sake.sparksparql

import java.io.File
import java.nio.charset.UnsupportedCharsetException
import java.sql.Timestamp
import org.apache.spark.sql.{SQLContext, Row, SaveMode}
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.spark.sql.DataFrame
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone

class SparqlSuite extends FunSuite with BeforeAndAfterAll {
  //val dbpediaEndpoint = "http://ustst018-cep-node1:3030/dbpedia/query"
  val dbpediaEndpoint = "mem:dataset=src/test/resources/dbpedia_extract.nt"
  
  private var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(new SparkContext("local[2]", "CsvSuite"))
  }

  override protected def afterAll(): Unit = {
    try {
      sqlContext.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("select1") {
    val frame = queryDbpedia("""SELECT ?object
      | WHERE {
      |   <http://dbpedia.org/ontology/> <http://purl.org/dc/terms/title> ?object
      | }""".stripMargin)
    assert(frame.columns === Array("object"))
    val rows = frame.select("object").collect()
    assert(rows.size === 1)
    assert(rows(0).getString(0) === "The DBpedia Ontology")
  }
  
  test("select *") {
    val frame = queryDbpedia("""PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | SELECT *
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | ?x rdfs:label ?label
      |   FILTER(LANG(?label) = "" || LANGMATCHES(LANG(?label), "en"))
      | }""".stripMargin)
    assert(frame.columns === Array("x", "label"))
    val rows = frame.select("x").collect()
    assert(rows.size === 9)
  }
  
  test("ask") {
    val frame = queryDbpedia("""PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | ASK
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | }""".stripMargin)
    assert(frame.columns === Array("ASK"))
    val rows = frame.select("ASK").collect()
    assert(rows.size === 1)
    assert(rows(0).getString(0) === "true")
  }
  
  test("ask with userschema") {
    val query = """PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | ASK
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | }""".stripMargin
    val frame = queryDbpedia(query, StructType(List(StructField("ASK", BooleanType, false))))
    assert(frame.columns === Array("ASK"))
    val rows = frame.select("ASK").collect()
    assert(rows.size === 1)
    assert(rows(0).getBoolean(0) === true)
  }

  test("construct") {
    val df = queryDbpedia("""PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | CONSTRUCT
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | }""".stripMargin)
    assert(df.columns === Array("Subject", "Predicate", "Object"))
    val rows = df.select("Subject", "Predicate", "Object").collect()
    assert(rows.size === 9)
    val tuples = rows.map { r => (r.getString(0), r.getString(1), r.getString(2)) }
    assert(tuples.count { t => t == ("http://dbpedia.org/ontology/SoccerLeague", 
        "http://www.w3.org/2000/01/rdf-schema#subClassOf",
        "http://dbpedia.org/ontology/SportsLeague") } === 1)
  }
  
  test("describe") {
    val df = queryDbpedia("""PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      | 
      | DESCRIBE ?x
      | WHERE {
      | ?x rdfs:subClassOf dbpo:SportsLeague.
      | ?x rdfs:label "soccer league"@en
      | }""".stripMargin)
    assert(df.columns === Array("Subject", "Predicate", "Object"))
    val rows = df.select("Subject", "Predicate", "Object").collect()
    assert(rows.size > 5)
    val tuples = rows.map { r => (r.getString(0), r.getString(1), r.getString(2)) }
    assert(tuples.count { t => t == ("http://dbpedia.org/ontology/SoccerLeague", 
        "http://www.w3.org/2000/01/rdf-schema#subClassOf",
        "http://dbpedia.org/ontology/SportsLeague") } === 1)
  }
  
  test("datatypes") {
    val query = """PREFIX dbpo: <http://dbpedia.org/ontology/>
      | PREFIX test: <http://research.usu.de/>
      | 
      | SELECT ?i ?d ?b ?s ?t
      | WHERE {
      |   test:test test:int ?i .
      |   test:test test:double ?d .
      |   test:test test:boolean ?b .
      |   test:test test:string ?s .
      |   test:test test:timestamp ?t .
      | }""".stripMargin
    val df = queryDbpedia(query, StructType(List(StructField("i", IntegerType, false),
        StructField("d", DoubleType, false),
        StructField("b", BooleanType, false),
        StructField("s", StringType, false),
        StructField("t", TimestampType, false))))
    
    assert(df.columns === Array("i", "d", "b", "s", "t"))
    val rows = df.select("i", "d", "b", "s", "t").collect()
    assert(rows.size === 1)
    val row = rows(0)
    val i = row.getInt(0)
    val d = row.getDouble(1)
    val b = row.getBoolean(2)
    val s = row.getString(3)
    assert(i === 100)
    assert(d === 1.25E-4)
    assert(b === false)
    assert(s === "foo")
    
    val t = row.getTimestamp(4)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
    val prettyDate = sdf.format(t)    
    assert(prettyDate === "2016-01-13 11:08:07")
  }
  
  private def queryDbpedia(query: String): DataFrame = {
    sqlContext.sparqlSelect(dbpediaEndpoint, query)
  }

  private def queryDbpedia(query: String, schema: StructType): DataFrame = {
    sqlContext.sparqlSelect(dbpediaEndpoint, query, userSchema = schema)
  }
}
