package ru.ksu.niimm.cll.anduin.sem

import org.specs.Specification
import com.twitter.scalding.{Tsv, TypedTsv, JobTest, TupleConversions}
import org.specs.runner.JUnit4
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
class IncomingLinkProcessorTest extends JUnit4(IncomingLinkProcessorTestSpec)

object IncomingLinkProcessorTestSpec extends Specification with TupleConversions {
  "The incoming link processor job" should {
    JobTest("ru.ksu.niimm.cll.anduin.sem.IncomingLinkProcessor").
      arg("input", "inputFile").
      arg("output", "outputFile").
      source(TypedTsv[(Context, Subject, Predicate, Range)]("inputFile"), List(
      // 1st row
      ("<http://somecontext.com/1>", "<http://eprints.rkbexplorer.com/id/caltech/eprints-7519>",
        "<http://www.aktors.org/ontology/portal#has-author>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>"),
      // 2nd row
      ("<http://somecontext.com/1>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>",
        "<http://www.aktors.org/ontology/portal#knows>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>"),
      // 3rd row
      ("<http://somecontext.com/1>", "<http://eprints.rkbexplorer.com/id/caltech/person-1>",
        "<http://www.aktors.org/ontology/portal#label>", "\"<body>No. 1 RNA researcher 1</body>\""),
      // 4th row
      ("<http://somecontext.com/4>", "<http://eprints.rkbexplorer.com/id/caltech/person-3>",
        "<http://www.aktors.org/ontology/portal#redirect>", "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>"),
      // 5th row
      ("<http://somecontext.com/4>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>",
        "<http://www.aktors.org/ontology/portal#value>", "\"<body><p>123</p></body>\""),
      // 6th row
      ("<http://somecontext.com/4>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>",
        "<http://www.aktors.org/ontology/portal#value>", "\"<body><p>123</p></body>\""),
      // 7th row
      ("<http://somecontext.com/4>", "<http://eprints.rkbexplorer.com/id/caltech/person-2>",
        "<http://www.aktors.org/ontology/portal#knows>", "_:p1"),
      // 8th row
      ("<http://somecontext.com/4>", "_:p1",
        "<http://www.aktors.org/ontology/portal#name>", "\"<body>Relevant name</body>\""),
      // 9th row
      ("<http://somecontext.com/5>", "_:p1",
        "<http://www.aktors.org/ontology/portal#name>", "Irrelevant name")
    )).
      sink[(Int, Subject, Range)](Tsv("outputFile")) {
      outputBuffer =>
        "output the correct entity descriptions" in {
          outputBuffer.size must_== 3
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-2>", "\"No. 1 RNA researcher 1\"")
          outputBuffer mustContain(3, "<http://eprints.rkbexplorer.com/id/caltech/person-1>", "eprints")
          outputBuffer mustContain(3, "<http://dbpedia.org/resource/Caldwell_High_School_(Caldwell,_Texas)>", "person")
        }
    }.run.
      finish
  }
}
