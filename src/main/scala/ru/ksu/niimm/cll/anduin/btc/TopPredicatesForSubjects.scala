package ru.ksu.niimm.cll.anduin.btc

import com.twitter.scalding.{Args, Job, TextLine, Tsv}
import ru.ksu.niimm.cll.anduin.util.NodeParser._

/**
 * @author Nikita Zhiltsov 
 */
class TopPredicatesForSubjects(args: Args) extends Job(args) {
  val RDF_TYPE_PREDICATE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

  private val inputFormat = args("inputFormat")

  def isNquad = inputFormat.equals("nquad")

  /**
   * reads the triples
   */
  private val triples = TextLine(args("input")).read.filter('line) {
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
  }.map('subject -> 'subject) {
    subject: String =>
      if (subject.startsWith("<") && subject.endsWith(">")) subject.drop(1).dropRight(1) else subject
  }

  val subjects = TextLine(args("subjects")).read.rename('line -> 'subject)

  val filteredTriples = triples.joinWithTiny('subject -> 'subject, subjects)

  val predCount = filteredTriples.groupBy('predicate) {
    _.size('count)
  }

  filteredTriples.joinWithSmaller('predicate -> 'predicate, predCount)
    .groupAll {
    _.sortBy(('count, 'predicate, 'subject, 'object)).reverse
  }.project(('count, 'predicate, 'subject, 'object)).write(Tsv(args("output")))
}
