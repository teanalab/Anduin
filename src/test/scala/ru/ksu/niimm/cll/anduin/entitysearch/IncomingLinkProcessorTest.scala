package ru.ksu.niimm.cll.anduin.entitysearch

import org.specs.Specification
import com.twitter.scalding._
import org.specs.runner.JUnit4
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov
 */
class IncomingLinkProcessorTest extends JUnit4(IncomingLinkProcessorTestSpec)

object IncomingLinkProcessorTestSpec extends Specification with TupleConversions {
  "The incoming link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.entitysearch.IncomingLinkProcessor").
      arg("inputFirstLevel", "inputFirstLevelFile").
      arg("inputSecondLevel", "inputSecondLevelFile").
      arg("inputFormat", "nquad").
      arg("output", "outputFile")
      .source(TypedTsv[(String, Subject, Range)]("inputSecondLevelFile"), List(
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"No. 1 RNA researcher 1\""),
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"<body><p>123</p></body>\"")
    ))
      .source(TextLine("inputFirstLevelFile"), List(
      // 1st row
      ("0", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519> <http://www.aktors.org/ontology/portal#has-author> <http://eprints.rkbexplorer.com/id/caltech/person-1> <http://context.com/1> ."),
      // 2nd row
      ("1", "<http://eprints.rkbexplorer.com/id/caltech/person-1> <http://www.aktors.org/ontology/portal#knows> <http://eprints.rkbexplorer.com/id/caltech/person-2> <http://context.com/1> ."),
      // 4th row
      ("2", "<http://eprints.rkbexplorer.com/id/caltech/person-3> <http://www.aktors.org/ontology/portal#redirect> <http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)> <http://context.com/1> .")
    )).
      sink[(Int, Subject, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 3
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "\"eprints\"")
          outputBuffer mustContain(3, "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "\"person\"")
        }
    }.run.
      finish
  }
}
