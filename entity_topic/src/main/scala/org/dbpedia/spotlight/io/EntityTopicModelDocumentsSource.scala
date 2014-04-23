package org.dbpedia.spotlight.io

import org.apache.commons.logging.LogFactory
import scala.concurrent.ExecutionContext
import org.dbpedia.spotlight.spot.Spotter
import org.dbpedia.spotlight.db.model.{SurfaceFormStore, ResourceStore, TextTokenizer}
import org.dbpedia.spotlight.db.WikipediaToDBpediaClosure
import org.dbpedia.spotlight.model.{Feature, DBpediaResourceOccurrence, Text}
import org.dbpedia.spotlight.exceptions.{NotADBpediaResourceException, DBpediaResourceNotFoundException, SurfaceFormNotFoundException}
import scala.collection.JavaConversions._
import scala.Array


/**
 * Created by dirkw on 3/11/14.
 */
object EntityTopicModelDocumentsSource {
    private final val log = LogFactory.getLog(getClass)

    private implicit val execontext = ExecutionContext.global

    def fromOccurrenceSource(occSource:OccurrenceSource,
                             spotter:Spotter,
                             tokenizer:TextTokenizer,
                             resStore:ResourceStore,
                             sfStore:SurfaceFormStore,
                             candidates:Array[Array[Int]],
                             wikiToDBpediaClosure:WikipediaToDBpediaClosure) = new Traversable[EntityTopicDocument] {

        def foreach[U](f: (EntityTopicDocument) => U): Unit = {
            def getDocument(currentContext:Text, currentAnnotations:List[DBpediaResourceOccurrence]) = {
                val tokens = tokenizer.tokenize(currentContext)
                currentContext.setFeature(new Feature("tokens", tokens))

                val spots = spotter.extract(currentContext)
                val entityToMentions =
                    (currentAnnotations.map(occ => {
                        var id = 0
                        try {
                            id = sfStore.getSurfaceForm(occ.surfaceForm.name).id
                        }
                        catch {
                            case ex:SurfaceFormNotFoundException => log.debug(ex.getMessage)
                        }
                        (Some(occ.resource), id)
                    }) ++ spots.withFilter(spot => !currentAnnotations.exists(_.textOffset == spot.textOffset)).map(spot => {
                        (None, spot.surfaceForm.id)
                    })).filter(pair => pair._2 > 0 && candidates(pair._2) != null && candidates(pair._2).length > 0)


                val document = EntityTopicDocument(
                    tokens.withFilter(_.tokenType.id > 0).map(_.tokenType.id).toArray,
                    tokens.withFilter(_.tokenType.id > 0).map(_ => Int.MinValue).toArray,
                    entityToMentions.map(_._2).toArray,
                    entityToMentions.map(em => em._1.map(e => {
                        var id = Int.MinValue
                        try {
                            val uri = wikiToDBpediaClosure.wikipediaToDBpediaURI(e.uri)
                            id = resStore.getResourceByName(uri).id
                        }
                        catch {
                            case ex: DBpediaResourceNotFoundException => log.debug(ex.getMessage)
                            case ex: NotADBpediaResourceException => log.debug(e.uri+" -> "+ex.getMessage)
                        }
                        id
                    }).getOrElse(Int.MinValue)).toArray,
                    entityToMentions.map(_ => Int.MinValue).toArray)

                document
            }

            var currentContext:Text = new Text("")
            var currentAnnotations = List[DBpediaResourceOccurrence]()
            //var future:Future[U] = null

            occSource.foreach(resOcc => {
                if(currentContext == resOcc.context) {
                    currentAnnotations ::= resOcc
                }
                else {
                    if(currentContext.text != "") {
                        //HACK: parallelize parsing and execution very simply
                        //if(future != null)
                        //    Await.result(future,Duration.Inf)
                        //future = Future {
                        val doc = getDocument(currentContext,currentAnnotations)
                        if(!doc.mentions.isEmpty)
                            f(doc)
                        //}
                    }

                    currentContext = resOcc.context
                    currentAnnotations = List(resOcc)
                }
            })
            getDocument(currentContext,currentAnnotations)
        }
    }

}

@SerialVersionUID(3891518562128537200L)
case class EntityTopicDocument(tokens:Array[Int],
                               tokenEntities:Array[Int],
                               mentions:Array[Int],
                               mentionEntities:Array[Int],
                               entityTopics:Array[Int]) extends Serializable {
    def this() = this(Array[Int](),Array[Int](),Array[Int](),Array[Int](),Array[Int]())
}

