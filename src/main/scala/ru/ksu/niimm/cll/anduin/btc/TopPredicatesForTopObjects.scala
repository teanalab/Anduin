package ru.ksu.niimm.cll.anduin.btc

import com.twitter.scalding.{Args, Job, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
class TopPredicatesForTopObjects(args: Args) extends Job(args) {
  val RDF_TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  /**
   * reads the triples
   */
  private val triples = TextLine(args("input")).read.filter('line){
    line: String =>
      val cleanLine = line.trim
      cleanLine.startsWith("<") || cleanLine.startsWith("_")
  }
    .mapTo('line ->('subject, 'predicate, 'object)) {
    line: String =>
      if (isNquad) {
        val nodes = extractNodes(line)
        (nodes._2, nodes._3, nodes._4)
      } else extractNodesFromN3(line)
  }.unique(('subject, 'predicate, 'object)).filter(('predicate, 'object)) {
    fields: (Predicate, Range) =>
      !fields._1.equals(RDF_TYPE_PREDICATE) && fields._2.startsWith("<")
  }

  val topObjects = triples.groupBy('object) {
    _.size('count)
  }.groupAll {
    _.sortedReverseTake[(Long,String)](('count, 'object) -> 'top, 100)
  }.flattenTo[(Long,String)]('top -> ('count, 'object)).project('object).debug

  val topObjectsPrecicates = triples.joinWithSmaller('object -> 'object, topObjects).unique('predicate, 'object)

  triples.joinWithTiny('object -> 'object, topObjects).groupBy('predicate) {
    _.size('count)
  }.joinWithLarger('predicate -> 'predicate, topObjectsPrecicates).groupAll {
    _.sortBy(('count,'predicate)).reverse
  }.write(Tsv(args("output")))
}



