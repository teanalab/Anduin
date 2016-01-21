package ru.ksu.niimm.cll.anduin.entitysearch

import com.twitter.scalding._
import ru.ksu.niimm.cll.anduin.util.NodeParser.Range
import ru.ksu.niimm.cll.anduin.util.NodeParser._
import cascading.pipe.joiner.{LeftJoin, InnerJoin}

/**
 * This processor resolves incoming links in entity descriptions
 *
 * @author Nikita Zhiltsov 
 */
class IncomingLinkProcessor(args: Args) extends Job(args) {
  val MAX_LINE_LENGTH = 100000
  private val inputFormat = args("inputFormat")
  def isNquad = inputFormat.equals("nquad")

  private val firstLevelEntities =
    TextLine(args("inputFirstLevel")).read
      .filter('line) {
        line: String =>
          val cleanLine = line.trim
          cleanLine.startsWith("<") && line.length < MAX_LINE_LENGTH
      }
      .mapTo('line ->('subject, 'predicate, 'object)) {
        line: String =>
          if (isNquad) {
            val nodes = extractNodes(line)
            (nodes._2, nodes._3, nodes._4)
          } else extractNodesFromN3(line)
      }
      .project(('subject, 'object))
      .filter(('subject, 'object)) {
      fields: (Subject, Range) => fields._1.startsWith("<") && fields._2.startsWith("<")
    }
  /**
   * filters second level entities with URIs as objects, 'name'-like predicates and literal values;
   * merges the literal values
   */
  private val secondLevelEntities =
    TypedTsv[(String, Subject, Range)](args("inputSecondLevel")).read.rename((0, 1, 2) ->('predicatetype, 'subject2, 'object2))
      .filter('predicatetype) {
      predicateType: String => predicateType.equals("0")
    }
      .project(('subject2, 'object2))

  firstLevelEntities
    .joinWithSmaller(('subject -> 'subject2), secondLevelEntities, joiner = new LeftJoin)
    .mapTo(('subject, 'object, 'subject2, 'object2) -> ('predicatetype, 'subject,'object)) {
    fields: (Subject, Range, Subject, Range) =>
      if (fields._4 == null) (3, fields._2, stripURI(fields._1).mkString("\"", "", "\"")) else (3, fields._2, fields._4)
  }
    .write(Tsv(args("output")))
}
