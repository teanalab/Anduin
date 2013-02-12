package ru.ksu.niimm.cll.anduin

import org.junit.runner.RunWith
import org.specs.runner.{JUnit4, JUnitSuiteRunner}
import org.specs.Specification
import com.twitter.scalding.{Tsv, TypedTsv, JobTest, TupleConversions}
import util.{FixedPathLzoTsv, FixedPathLzoTextLine}

/**
 * @author Nikita Zhiltsov 
 */
@RunWith(classOf[JUnitSuiteRunner])
class EntityTypeProcessorTest extends JUnit4(EntityTypeProcessorTestSpec)

object EntityTypeProcessorTestSpec extends Specification with TupleConversions {
  "Entity type processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.EntityTypeProcessor").
      arg("input", "inputFile").
      arg("inputTermEntityPairs", "inputTermEntityPairsFile").
      arg("inputTypeList", "inputTypeListFile").
      arg("output", "outputFile").
      source(TypedTsv[(Int, String)]("inputTypeListFile"), List(
      (0, "<http://xmlns.com/foaf/0.1/Person>"),
      (1, "<http://rdfs.org/sioc/types#WikiArticle>"),
      (2, "<http://purl.org/rss/1.0/item>")
    ))
      .source(TypedTsv[(String, String)]("inputTermEntityPairsFile"), List(
      ("person", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      ("article", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>")
    ))
      .source(new FixedPathLzoTextLine("inputFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/rss/1.0/item> " +
        "<http://somecontext.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://somecontext.com/1> ."),
      // 3rd row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://somecontext.com/6> ."),
      // 4th row
      ("3", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/4> ."),
      // 5th row
      ("4", "<http://eprints.rkbexplorer.com/id/caltech/person-1> " +
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://rdfs.org/sioc/types#WikiArticle> <http://somecontext.com/10> .")
    )).
      sink[(String, Int, Int)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity types" in {
          outputBuffer.size must_== 3
          outputBuffer mustContain("person", 0, 1)
          outputBuffer mustContain("person", 1, 1)
          outputBuffer mustContain("article", 2, 1)
        }
    }.run.
      finish
  }
}
