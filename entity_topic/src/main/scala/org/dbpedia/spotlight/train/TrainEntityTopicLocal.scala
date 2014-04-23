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
import akka.actor.{ActorDSL, ActorSystem, Actor}
import akka.pattern._
import org.dbpedia.spotlight.io.{EntityTopicModelDocumentsSource, AllOccurrenceSource, EntityTopicDocument}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.dbpedia.extraction.util.Language
import org.dbpedia.spotlight.model.SimpleEntityTopicModel
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.util.Timeout


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
        System.setProperty("spark.cleaner.ttl","600000")
        val kryo = newKryo

        new EntityTopicKryoRegistrator().registerClasses(kryo)

        val wikidump = new File(args(0))
        val modelDir = new File(args(1))

        val numTopics = args(2).toInt
        val dataDir = new File(args(3))

        val cacheSize = 1000

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
        
        def writeDocuments(dir: File, docs: Array[EntityTopicDocument]) {
            val out = new Output(new FileOutputStream(new File(dir, "etd_" + System.currentTimeMillis())))
            kryo.writeObject(out, docs)
            out.close()
        }

        var init = true
        var dir:File = new File(dataDir,s"etd_0")
        dir.listFiles().foreach(_.delete())

        implicit val system = ActorSystem("ET")

        class WriterActor extends Actor {
            var cached = new Array[EntityTopicDocument](cacheSize)
            var currentCacheSize = 0
            var counter = 0
            def receive = {
                case doc:EntityTopicDocument =>
                    cached(currentCacheSize) = doc
                    currentCacheSize += 1
                    if(currentCacheSize == cacheSize) {
                        writeDocuments(dir, cached)
                        counter += 1
                        currentCacheSize = 0
                        LOG.info((counter*cacheSize) + " entity-topic-documents written")
                    }
            }
        }

        val writer = ActorDSL.actor(new WriterActor)

        class SamplerActor extends Actor {
            var counter = 0
            def receive = {
                case doc: EntityTopicDocument =>
                    model.gibbsSampleDocument(doc, withUpdate= true, init)
                    writer ! doc
                    counter += 1
                    if(counter % cacheSize == 0)
                        LOG.info(counter + " entity-topic-documents sampled")
                case "done?" => sender ! "yeah"
            }
        }

        val sampler = ActorDSL.actor(new SamplerActor)

        var ctr = 0
        val occSource = AllOccurrenceSource.fromXMLDumpFile(wikidump,Language.English)
        EntityTopicModelDocumentsSource.fromOccurrenceSource(occSource,
            spotter,tokenizer,
            resStore,sfStore,candMapStore.asInstanceOf[MemoryCandidateMapStore].candidates,
            wikipediaToDBpediaClosure).foreach( doc => {

            sampler ! doc

            ctr += 1
            if(ctr % cacheSize == 0) {
                LOG.info(ctr + " entity-topic-documents pushed")
            }
        })

        init = false
        //Start streaming
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
                        LOG.info(ctr + " entity-topic-documents pushed")
                    }
                })
            })
            Await.result((sampler ? "done?")(Timeout(300000)),Duration.apply(300,scala.concurrent.duration.SECONDS))
        })

        System.exit(0)
    }

}
