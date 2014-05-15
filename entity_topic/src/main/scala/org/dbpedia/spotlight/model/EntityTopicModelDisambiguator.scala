package org.dbpedia.spotlight.model

import org.dbpedia.spotlight.disambiguate.ParagraphDisambiguator
import org.dbpedia.spotlight.exceptions.{InputException, ItemNotFoundException, SearchException}
import org.dbpedia.spotlight.db.model.{TextTokenizer, SurfaceFormStore, ResourceStore}

/**
 * @author dirk
 *         Date: 5/15/14
 *         Time: 9:41 AM
 */
class EntityTopicModelDisambiguator(val model:SimpleEntityTopicModel,
                                    val resStore:ResourceStore,
                                    val sfStore:SurfaceFormStore,
                                    val tokenizer:TextTokenizer = null) extends ParagraphDisambiguator {
  /**
   * Executes disambiguation per paragraph (collection of occurrences).
   * Can be seen as a classification task: unlabeled instances in, labeled instances out.
   *
   * @param paragraph
   * @return
   * @throws SearchException
   * @throws InputException
   */
  def disambiguate(paragraph: Paragraph) = {
    val best = bestK(paragraph,1)
    paragraph.occurrences.map(sfOcc => best.get(sfOcc).map(_.headOption.getOrElse(null)).getOrElse(null))
  }

  /**
   * Executes disambiguation per occurrence, returns a list of possible candidates.
   * Can be seen as a ranking (rather than classification) task: query instance in, ranked list of target URIs out.
   *
   * @param sfOccurrences
   * @param k
   * @return
   * @throws SearchException
   * @throws ItemNotFoundException    when a surface form is not in the index
   * @throws InputException
   */
  def bestK(paragraph: Paragraph, k: Int): Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]] = {
    paragraph.occurrences.foreach(occ => if(occ.surfaceForm.id < 0) occ.surfaceForm.id = sfStore.getSurfaceForm(occ.surfaceForm.name).id)
    if(tokenizer != null)
      tokenizer.tokenizeMaybe(paragraph.text)
    val doc = EntityTopicDocument.fromParagraph(paragraph)
    //Burn In
    model.gibbsSampleDocument(doc,10)

    //Take samples
    val iterations = 100
    val stats = model.gibbsSampleDocument(doc, iterations, returnStatistics = true)

    paragraph.occurrences.zip(stats).foldLeft(Map[SurfaceFormOccurrence, List[DBpediaResourceOccurrence]]()) {
      case (acc,(sfOcc,counts)) =>
        val chosen = counts.toSeq.sortBy(-_._2).take(k)

        acc + (sfOcc -> chosen.map {case (resId, count) => new DBpediaResourceOccurrence(
          "",resStore.getResource(resId),
          sfOcc.surfaceForm,
          sfOcc.context,
          sfOcc.textOffset,
          contextualScore = count.toDouble/iterations)
        }.toList)
    }
  }

  /**
   * Every disambiguator has a name that describes its settings (used in evaluation to compare results)
   * @return a short description of the Disambiguator
   */
  def name = "Entity-Topic-Model-Disambiguator"

}
