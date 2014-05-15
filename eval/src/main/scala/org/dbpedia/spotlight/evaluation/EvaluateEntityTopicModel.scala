package org.dbpedia.spotlight.evaluation

import org.dbpedia.spotlight.corpus.MilneWittenCorpus
import java.io.{FileInputStream, File}
import org.dbpedia.spotlight.model.{AnnotatedParagraph, EntityTopicModelDisambiguator, SimpleEntityTopicModel}
import org.dbpedia.spotlight.db.{FSASpotter, WikipediaToDBpediaClosure, SpotlightModel}
import java.util.Locale
import org.dbpedia.spotlight.db.memory.MemoryStore
import org.dbpedia.spotlight.spot.Spotter
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer
import org.dbpedia.spotlight.db.stem.SnowballStemmer

/**
 * @author dirk
 *          Date: 5/15/14
 *          Time: 10:19 AM
 */
object EvaluateEntityTopicModel {

  def main(args:Array[String]) {
    val corpus = MilneWittenCorpus.fromDirectory(new File(args(0)))
    val modelDir = new File(args(1))
    val entityTopicModel = SimpleEntityTopicModel.fromFile(new File(args(2)))

    val (tokenStore, sfStore, resStore, _, _) = SpotlightModel.storesFromFolder(modelDir)

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


    val mappedCorpus = corpus.mapConserve(p => {
      p.occurrences.foreach(occ => {
        val uri = wikipediaToDBpediaClosure.wikipediaToDBpediaURI(occ.resource.uri)
        occ.resource.id = resStore.getResourceByName(uri).id
      } )
      p
    })

    val disambiguator = new EntityTopicModelDisambiguator(entityTopicModel,resStore,sfStore,tokenizer)

    EvaluateParagraphDisambiguator.evaluate(mappedCorpus, disambiguator, List(),List())
  }

}
