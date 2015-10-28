package ru.ksu.niimm.cll.anduin.entitysearch

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import ru.ksu.niimm.cll.anduin.util.FixedPathLzoTextLine
import ru.ksu.niimm.cll.anduin.util.PredicateGroupCodes._


/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class NquadEntityAttributeWithFilteringProcessorTest extends JUnit4(NquadEntityAttributeWithFilteringProcessorTestSpec)

object NquadEntityAttributeWithFilteringProcessorTestSpec extends Specification with TupleConversions {
  "The name-like attribute processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.EntityAttributeWithFilteringProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      arg("inputFormat", "nquad").
      arg("inputPredicates", "inputPredicatesFile").
      arg("entityNames", "entityNamesFile")
      .source(TypedTsv[(String, String)]("inputPredicatesFile"), List(
      ("0", "<http://www.w3.org/2000/01/rdf-schema#label>"),
      ("1", "<http://www.aktors.org/ontology/portal#value>"),
      ("2", "<http://dbpedia.org/ontology/title>"),
      ("3", "<http://dbpedia.org/ontology/office>"),
      ("4", "<http://purl.org/dc/terms/subject>"),
      ("5", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"),
      ("6", "<http://www.aktors.org/ontology/portal#redirect>"),
      ("7", DBPEDIA_WIKI_PAGE_WIKI_LINK)
    ))
      .source(TypedTsv[(String, String)]("entityNamesFile"), List(
      ("<http://dbpedia.org/resource/Author>", "\"Author\"@en"),
      ("<http://dbpedia.org/resource/Category:American_physicists>", "American physicists"),
      ("<http://dbpedia.org/ontology/Scientist>", "Scientist"),
      ("<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "Caldwell High School"),
      ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"type\"@en"),
      ("<http://www.w3.org/2000/01/rdf-schema#label>", "\"label\"@en"),
      ("<http://www.aktors.org/ontology/portal#redirect>", "redirect"),
      ("<http://www.aktors.org/ontology/portal#has-author>", "has author"),
      ("<http://dbpedia.org/ontology/title>", "title")
    ))
      .source(TextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://example.com/context> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://example.com/context> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1\" <http://example.com/context> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> <http://example.com/context> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>123</p></body>\" <http://example.com/context> ."),
      // 6th row
      ("5", "<http://eprints.rkbexplorer.com/id/caltech/person-2> " +
        "<http://www.aktors.org/ontology/portal#value> \"<body><p>321</p></body>\" <http://example.com/context> ."),
      // 7th row
      ("6", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/2000/01/rdf-schema#label> \"No. 1 RNA researcher 1 Fr\"@fr <http://example.com/context> ."),
      // 8th row
      ("7", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/title> \"Researcher\"@en <http://example.com/context> ."),
      // 9th row
      ("8", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/office> <http://dbpedia.org/resource/Author> <http://example.com/context> ."),
      // 10th row
      ("9", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://purl.org/dc/terms/subject> <http://dbpedia.org/resource/Category:American_physicists> <http://example.com/context> ."),
      // 11th row
      ("10", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Scientist> <http://example.com/context> ."),
      // 12th row
      ("11", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://dbpedia.org/ontology/wikiPageWikiLink> <http://dbpedia.org/ontology/Scientist> <http://example.com/context> .")
    )).
      sink[(Int, Subject, ru.ksu.niimm.cll.anduin.util.NodeParser.Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 8
          outputBuffer mustContain(NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Researcher\"@en")
          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"321\"")
          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"123\"")
          outputBuffer mustContain(CATEGORIES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "American physicists")
          outputBuffer mustContain(OUTGOING_ENTITY_NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"type\"@en Scientist")
          outputBuffer mustContain(OUTGOING_ENTITY_NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "redirect Caldwell High School")
          outputBuffer mustContain(OUTGOING_ENTITY_NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"Author\"@en")
//          outputBuffer.size must_== 4
//          outputBuffer mustContain(NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\" \"Researcher\"@en")
//          outputBuffer mustContain(ATTRIBUTES, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"321\" \"123\"")
//          outputBuffer mustContain(CATEGORIES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "American physicists")
//          outputBuffer mustContain(OUTGOING_ENTITY_NAMES, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"type\"@en Scientist redirect Caldwell High School \"Author\"@en")
        }
    }.run.
      finish
  }
}


