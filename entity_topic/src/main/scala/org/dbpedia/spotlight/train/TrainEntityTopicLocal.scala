package org.dbpedia.spotlight.train

import org.apache.commons.logging.LogFactory
import com.esotericsoftware.kryo.Kryo
import java.io.{FileOutputStream, FileInputStream, File}
import java.util.Locale
import org.dbpedia.spotlight.db.{FSASpotter, SpotlightModel, WikipediaToDBpediaClosure}
import org.dbpedia.spotlight.db.memory.{MemoryResourceStore, MemoryStore, MemoryCandidateMapStore, MemorySurfaceFormStore}
import org.dbpedia.spotlight.spot.Spotter
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer
import org.dbpedia.spotlight.db.stem.SnowballStemmer
import akka.actor._
import akka.pattern._
import org.dbpedia.spotlight.io.AllOccurrenceSource
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.util.Language
import org.dbpedia.spotlight.model._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.util.Timeout
import akka.routing.RoundRobinRouter
import scala.collection.JavaConversions._
import org.dbpedia.spotlight.exceptions.{SurfaceFormNotFoundException, NotADBpediaResourceException, DBpediaResourceNotFoundException}
import scala.Some
import org.dbpedia.spotlight.io.EntityTopicDocument
import org.dbpedia.spotlight.db.model.{SurfaceFormStore, ResourceStore, TextTokenizer}


/**
 * @author dirk
 *          Date: 4/11/14
 *          Time: 4:49 PM
 */
object TrainEntityTopicLocal {

    private val LOG = LogFactory.getLog(getClass)

    private[train] def newKryo = {
        val kryo = new Kryo()
        kryo.register(classOf[Array[EntityTopicDocument]])
        kryo
    }

    def main(args:Array[String]) {
        val kryo = newKryo

        val wikidump = new File(args(0))
        val modelDir = new File(args(1))

        val numTopics = args(2).toInt
        val dataDir = new File(args(3))

        val parallelism = if(args.size > 4) args(4).toInt else Runtime.getRuntime.availableProcessors()

        val cacheSize = 10000

        //currently only en
        val locale = new Locale("en", "US")
        val namespace = if (locale.getLanguage.equals("en")) {
            "http://dbpedia.org/resource/"
        } else {
            "http://%s.dbpedia.org/resource/"
        }

        val wikipediaToDBpediaClosure = new WikipediaToDBpediaClosure(
            namespace,
            new FileInputStream(new File(modelDir, "redirects.nt")),
            new FileInputStream(new File(modelDir, "disambiguations.nt"))
        )

        val (tokenStore,sfStore,resStore,candMapStore,_) = SpotlightModel.storesFromFolder(modelDir)
        val vocabSize = tokenStore.getVocabularySize
        val numMentions = sfStore.asInstanceOf[MemorySurfaceFormStore].size
        val numEntities = resStore.asInstanceOf[MemoryResourceStore].size

        val candMap = candMapStore.asInstanceOf[MemoryCandidateMapStore]

        val beta = 0.1
        val gamma = 0.0001
        val delta =  2000.0 / vocabSize
        val alpha = 50.0/numTopics

        val stopwords: Set[String] = SpotlightModel.loadStopwords(modelDir)

        lazy val spotter = {
            val dict = MemoryStore.loadFSADictionary(new FileInputStream(new File(modelDir, "fsa_dict.mem")))
            new FSASpotter(
                dict,
                sfStore,
                Some(SpotlightModel.loadSpotterThresholds(new File(modelDir, "spotter_thresholds.txt"))),
                stopwords
            ).asInstanceOf[Spotter]
        }

        val tokenizer =
            new LanguageIndependentTokenizer(stopwords, new SnowballStemmer("EnglishStemmer"), locale, tokenStore)

        val model = new SimpleEntityTopicModel(numTopics, numEntities, vocabSize, numMentions, candMap, alpha, beta, gamma, delta)

        dir = new File(dataDir,s"etd_0")
        dir.mkdirs()
        dir.listFiles().foreach(_.delete())

        implicit val system = ActorSystem("ET")

        val writer = ActorDSL.actor(new WriterActor(cacheSize,kryo))
        var sampler = system.actorOf(Props(classOf[SamplerActor],  model, writer).withRouter(RoundRobinRouter(math.max(1,parallelism/2-1))))
        val parser = system.actorOf(Props(classOf[ParserActor],  sampler, spotter,tokenizer,
            resStore,sfStore,candMapStore.asInstanceOf[MemoryCandidateMapStore].candidates,
            wikipediaToDBpediaClosure).withRouter(RoundRobinRouter(math.max(1,parallelism/2-1))))


        var ctr = 0
        val occSource = AllOccurrenceSource.fromXMLDumpFile(wikidump,Language.English)

        var currentContext:Text = new Text("")
        var currentAnnotations = List[DBpediaResourceOccurrence]()
        occSource.foreach(resOcc => {
            if(currentContext == resOcc.context) currentAnnotations ::= resOcc
            else {
                if(currentContext.text != "" && !currentAnnotations.isEmpty) {
                    parser ! (currentContext,currentAnnotations)
                    ctr += 1
                    if(ctr % cacheSize == 0) {
                        sampler ! ctr
                        LOG.info(s"$ctr entity topic documents pushed")
                    }
                }

                currentContext = resOcc.context
                currentAnnotations = List(resOcc)
            }
        })
        parser ! (currentContext,currentAnnotations)
        Await.result((sampler ? "done?")(Timeout(300000)),Duration.apply(300,scala.concurrent.duration.SECONDS))
        system.stop(sampler)
        sampler = system.actorOf(Props(classOf[SamplerActor],  model, writer).withRouter(RoundRobinRouter(math.max(1,parallelism-2))))

        init = false
        (1 until 500).foreach(i => {
            val oldDir = dir
            dir = new File(dataDir,s"etd_$i")
            dir.mkdirs()
            dir.listFiles().foreach(_.delete())

            oldDir.listFiles().foreach(file => {
                val in = new Input(new FileInputStream(file))
                val docs = kryo.readObject(in,classOf[Array[EntityTopicDocument]])
                docs.foreach(doc => {
                    sampler ! doc
                    ctr += 1
                    if(ctr % cacheSize == 0) {
                      sampler ! ctr
                      LOG.info(ctr + " entity-topic-documents pushed")
                    }
                })
                Await.result((sampler ? "done?")(Timeout(300000)),Duration.apply(300,scala.concurrent.duration.SECONDS))
            })
            Await.result((sampler ? "done?")(Timeout(300000)),Duration.apply(300,scala.concurrent.duration.SECONDS))
        })

        System.exit(0)
    }

    var init = true
    var dir:File = null

    private class SamplerActor(model:SimpleEntityTopicModel, writer:ActorRef) extends Actor {
        var counter = 0
        def receive = {
            case doc: EntityTopicDocument =>
                model.gibbsSampleDocument(doc, withUpdate= true, init)
                writer ! doc
            case counter:Int => LOG.info(counter + " entity-topic-documents sampled")
            case "done?" => sender ! "yeah"
        }
    }
    private class WriterActor(cacheSize:Int,kryo:Kryo) extends Actor {
      var cached = new Array[EntityTopicDocument](cacheSize)
      var currentCacheSize = 0
      var counter = 0
      def receive = {
        case doc:EntityTopicDocument =>
          cached(currentCacheSize) = doc
          currentCacheSize += 1
          if(currentCacheSize == cacheSize) {
            writeDocuments(dir, cached, kryo)
            counter += 1
            currentCacheSize = 0
            LOG.info((counter*cacheSize) + " entity-topic-documents written")
          }
      }
    }
    private def writeDocuments(dir: File, docs: Array[EntityTopicDocument],kryo:Kryo) {
      val out = new Output(new FileOutputStream(new File(dir, "etd_" + System.currentTimeMillis())))
      kryo.writeObject(out, docs)
      out.close()
    }

    private class ParserActor(sampler:ActorRef,spotter:Spotter,
                              tokenizer:TextTokenizer,
                              resStore:ResourceStore,
                              sfStore:SurfaceFormStore,
                              candidates:Array[Array[Int]],
                              wikiToDBpediaClosure:WikipediaToDBpediaClosure) extends Actor {

        final val log = LogFactory.getLog(getClass)
        def getResourceId(e:DBpediaResource)= {
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
        }

        def getDocument(currentContext:Text, currentAnnotations:List[DBpediaResourceOccurrence]) = {
            val tokens = tokenizer.tokenize(currentContext)
            currentContext.setFeature(new Feature("tokens", tokens))

            val spots = spotter.extract(currentContext)

            val anchors =
                currentAnnotations.foldLeft( (List[Int](),List[Int]()) ) { case ((resourceIds,sfIds),occ) =>
                    var id = 0
                    try {
                        id = sfStore.getSurfaceForm(occ.surfaceForm.name).id
                    }
                    catch {
                        case ex:SurfaceFormNotFoundException => log.debug(ex.getMessage)
                    }
                    if(id > 0 && candidates(id) != null && candidates(id).length > 0)
                        (getResourceId(occ.resource) :: resourceIds, id :: sfIds)
                    else (resourceIds,sfIds)
                }

            val (resources,mentions) = spots.foldLeft(anchors){ case ((resourceIds,sfIds),spot) =>
                val id = spot.surfaceForm.id
                if(id > 0 && candidates(id) != null && candidates(id).length > 0 && !currentAnnotations.exists(_.textOffset == spot.textOffset))
                    (Int.MinValue :: resourceIds, id :: sfIds)
                else (resourceIds,sfIds)
            }

            val mentionEntities = resources.toArray
            val tokenArray = tokens.filter(_.tokenType.id > 0).toArray

            val document = EntityTopicDocument(
                tokenArray.map(_.tokenType.id),
                tokenArray.map(_ => Int.MinValue),
                mentions.toArray,
                mentionEntities,
                mentionEntities.map(_ => Int.MinValue))

            document
        }

        def receive = {
            case (currentContext:Text, currentAnnotations:List[DBpediaResourceOccurrence]) =>
                val doc = getDocument(currentContext,currentAnnotations)
                sampler ! doc
        }
    }

}
