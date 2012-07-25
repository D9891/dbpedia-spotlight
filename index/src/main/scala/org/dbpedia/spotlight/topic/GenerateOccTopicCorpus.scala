package org.dbpedia.spotlight.topic

import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import scala.collection.mutable.{Set, Map}
import scala.util.control.Breaks._
import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.util.IndexingConfiguration
import org.dbpedia.spotlight.io.{FileOccurrenceSource, FileOccsCategoriesSource}
import org.dbpedia.spotlight.model.{Topic, DBpediaCategory}
import utility.TopicUtil
import wikipedia.util.{WikipediaFlattenedHierarchyLoader, WikipediaHierarchyLoader}


/**
 * This object takes the splitted occs file extracted with SplitOccsByTopics$,
 * dbpedias sorted file article_categories (http://downloads.dbpedia.org/3.7/en/article_categories_en.nt.bz2),
 * wikipedias hierarchy (http://downloads.dbpedia.org/3.7/en/skos_categories_en.nt),
 * the output directory from FlattenWikipediaHierarchy, the number of examples each corpus should contain and finally
 * the output file, where the corpus will be written. Note that this corpus should be shuffled afterwards
 *
 * @author Dirk Weißenborn
 */
object GenerateOccTopicCorpus {

  private val LOG = LogFactory.getLog(getClass)

  /**
   *
   * @param args 1st: indexing.properties, 2nd: path to splitted occs,
   *             3rd: number of examples to be written for each topic (if <= 0, maximum number will be written),
   *             4th: output corpus file, 5th: from flattened hierarchy (true|false)
   */
  def main(args: Array[String]) {
    val config = new IndexingConfiguration(args(0))

    if (args.length < 5 || args(4).equals("true"))
      generateCorpusFromTopics(args(1), config.get("org.dbpedia.spotlight.data.sortedArticlesCategories"),
        config.get("org.dbpedia.spotlight.topic.flattenedHierarchy"),
        args(2).toInt, args(3))
    else
      generateCorpus(args(1), args(2).toInt, args(3))
  }

  /**
   * @deprecated This method was an old implementation based on occs that were splitted using the wikipedia hierarchy
   * @param pathToSplittedOccs
   * @param pathToArtCat
   * @param pathToHierarchy
   * @param pathToFlattenedHierarchy
   * @param nrOfExamples number of examples extracted from splitted occurences for corpus
   * @param outputPath
   */
  def generateCorpusFromHierarchy(pathToSplittedOccs: String, pathToArtCat: String, pathToHierarchy: String, pathToFlattenedHierarchy: String, nrOfExamples: Int, outputPath: String) {
    val hierarchy = WikipediaHierarchyLoader.loadCategoryHierarchy(pathToHierarchy)
    val outputWriter = new PrintWriter(new FileWriter(outputPath))
    val flattenedHierarchy = WikipediaFlattenedHierarchyLoader.loadFlattenedHierarchy(pathToFlattenedHierarchy)

    val parentFile = new File(pathToSplittedOccs)

    var corpusSize = 0
    if (nrOfExamples < 1) {
      corpusSize = Int.MaxValue
      parentFile.listFiles().foreach(topicFile => {
        var lineNr = 0
        Source.fromFile(topicFile).getLines().foreach(_ => lineNr += 1)
        corpusSize = math.min(lineNr, corpusSize)
      })
    }
    else
      corpusSize = nrOfExamples

    parentFile.listFiles().foreach(topicFile => {
      LOG.info("======================= Processing " + topicFile.getName + " =======================")

      val topic = new Topic(topicFile.getName.substring(0, topicFile.getName.length - 4))

      if (!topic.equals("others")) {
        /*val occsIt = Source.fromFile(topicFile).getLines()
        var counter = 0

        while(occsIt.hasNext&&counter < corpusSize) {
          outputWriter.println(topic+"\t"+occsIt.next.split("\t")(3))
          counter += 1
          if (counter%10000==0)
            LOG.info(counter+" examples written")
        }
      }
      else { */
        val topicCats = flattenedHierarchy(topic)
        val topCats = topicCats.filter {
          case (category, topicDistance) => topicDistance.equals(0.0)
        }.keySet

        var counter = 0
        var filteredCats: Set[DBpediaCategory] = null

        LOG.info("======================= Count category members =======================")
        var memberMap = Map[DBpediaCategory, Double]()

        //count members of each category
        FileOccsCategoriesSource.fromFile(topicFile, new File(pathToArtCat)).foreach {
          case (resourceOcc, categories) => {
            counter += 1

            filteredCats = categories.filter(category => topicCats.contains(category))
            filteredCats.foreach(category => {
              if (!memberMap.contains(category))
                memberMap += (category -> 0.0)
              memberMap(category) += 1.0 / filteredCats.size
            })

            if (counter % 100000 == 0)
              LOG.info(counter + " occs processed")

          }
        }

        LOG.info("======================= Prepare subcategories =======================")

        var subcategories: Map[DBpediaCategory, CategoryTuple] = Map()
        topCats.foreach(topCat => {
          var queue: Set[DBpediaCategory] = Set(topCat)
          var newqueue: Set[DBpediaCategory] = Set()
          var revisit: Set[DBpediaCategory] = Set()
          var subcats: Map[DBpediaCategory, CategoryTuple] = Map(topCat -> new CategoryTuple(new DBpediaCategory(""), Set(), false, math.ceil(corpusSize / topCats.size.toDouble).toInt, memberMap.getOrElse(topCat, 0.0)))

          def visitCategory(tuple: CategoryTuple, cat: DBpediaCategory) {
            revisit.remove(cat)
            if (tuple.nrPages > 0)
              try {
                val children: Set[DBpediaCategory] = hierarchy(cat)
                val subcatsNr = children.size

                //distribute left pages for this category equally to other category
                if (tuple.full || (subcatsNr == 0 && tuple.members <= tuple.nrPages)) {
                  tuple.full = true
                  val toDistribute: Float = (tuple.nrPages - tuple.members).toFloat
                  tuple.nrPages = tuple.members
                  var currentCat = tuple
                  var sum = 0
                  while (sum == 0 && currentCat.parent.getCategory != "" && toDistribute > 0) {
                    currentCat = subcats(currentCat.parent)
                    currentCat.children.foreach(child => {
                      val cat = subcats(child)
                      if (!cat.full)
                        sum += 1
                    })
                    if (sum == 0)
                      currentCat.full = true
                  }
                  if (sum > 0) {
                    currentCat.children.foreach(child => {
                      val subcat = subcats(child)
                      if (!subcat.full) {
                        subcat.nrPages += math.ceil(toDistribute / sum).toInt

                        if (subcat.members < subcat.nrPages)
                          revisit += (child)
                      }
                    })
                  }
                }

                //get subcategories if there are any
                if (subcatsNr > 0 && !tuple.full) {
                  val subcatPages = math.max(0, (tuple.nrPages - tuple.members).toFloat)
                  if (subcatPages > 0)
                    tuple.nrPages = tuple.members

                  if (tuple.children.size == 0) {
                    //if not, children were already added and this is a revisit
                    children.foreach(subCat => {
                      if (!subcats.contains(subCat))
                        tuple.children += (subCat)
                    })
                    val length = tuple.children.size
                    tuple.children.foreach(child => {
                      subcats += (child -> new CategoryTuple(cat, Set[DBpediaCategory](), false, math.ceil(subcatPages / length).toInt, memberMap.getOrElse(child, 0.0)))
                      newqueue += (child)
                    })
                  }
                  else {
                    tuple.children.foreach(child => {
                      val c = subcats(child)
                      c.nrPages += math.ceil(subcatPages / tuple.children.size).toInt
                      if (c.children.size > 0 && c.members <= c.nrPages) //means that c was already visited
                        revisit += (child)

                    })
                  }
                }
              }
              catch {
                case e: Exception =>;
              }
          }

          while (queue.size > 0) {
            newqueue = Set()
            queue.foreach(cat => {
              val tuple = subcats(cat)
              visitCategory(tuple, cat)

            })
            while (revisit.size > 0) {
              val revisitTemp = revisit.clone()
              revisitTemp.foreach(cat => visitCategory(subcats(cat), cat))
            }
            queue = newqueue
          }

          subcategories ++= subcats
        })

        LOG.info("======================= Extract " + corpusSize + " occs to corpus =======================")
        counter = 0

        var written = false

        breakable {
          FileOccsCategoriesSource.fromFile(topicFile, new File(pathToArtCat)).foreach {
            case (occ, categories) => {
              if (counter < corpusSize) {
                written = false

                filteredCats = categories.filter(category => topicCats.contains(category))
                filteredCats.foreach(category => {
                  val tuple: CategoryTuple = subcategories.getOrElse(category, null)
                  if (tuple != null && tuple.nrPages > 0) {
                    counter += 1

                    if (!written) {
                      outputWriter.println(topic.getName + "\t" + occ.context.text)
                      written = true
                    }

                    tuple.nrPages -= 1 / filteredCats.size

                    if (counter % 10000 == 0)
                      LOG.info(counter + " examples written")
                  }
                })
              }
              else
                break()

            }
          }
        }

        outputWriter.flush()
      }
    })

    outputWriter.close()
  }

  /**
   * Generates a text corpus of form "topic'\t'text" from splitted occurences, which can then be processed by TextCorpusToInputCorpus
   * @param pathToSplittedOccs
   * @param pathToArtCat
   * @param pathToFlattenedHierarchy
   * @param nrOfExamples -1 means write corpus with maximal number of examples
   * @param outputPath
   */
  def generateCorpusFromTopics(pathToSplittedOccs: String, pathToArtCat: String, pathToFlattenedHierarchy: String, nrOfExamples: Int, outputPath: String) {
    val outputWriter = new PrintWriter(new FileWriter(outputPath))
    val flattenedHierarchy = WikipediaFlattenedHierarchyLoader.loadFlattenedHierarchy(pathToFlattenedHierarchy)

    val parentFile = new File(pathToSplittedOccs)

    var corpusSize = 0
    if (nrOfExamples < 1) {
      corpusSize = Int.MaxValue
      parentFile.listFiles().foreach(topicFile => {
        var lineNr = 0
        Source.fromFile(topicFile).getLines().foreach(_ => lineNr += 1)
        corpusSize = math.min(lineNr, corpusSize)
      })
    }
    else
      corpusSize = nrOfExamples

    LOG.info("Writing corpus with size " + corpusSize + " for each topic")

    parentFile.listFiles().foreach(topicFile => {
      val topic = new Topic(topicFile.getName.substring(0, topicFile.getName.length - 4))

      LOG.info("======================= Processing " + topicFile.getName + " =======================")

      if (!topic.getName.equals(TopicUtil.CATCH_TOPIC)) {
        /*var counter = 0

        breakable { FileOccurrenceSource.fromFile(topicFile).foreach( occ => {
          if(counter < corpusSize) {
            outputWriter.println(topic.getName+"\t"+occ.context.text)
            counter += 1
            if (counter%10000==0)
              LOG.info(counter+" examples written")
          }
          else
            break()
        } ) }
      }
      else {*/
        val topicCats = flattenedHierarchy(topic)

        LOG.info("======================= Count category members =======================")
        var memberMap = Map[DBpediaCategory, Double]()
        var filteredCats: Set[DBpediaCategory] = null
        var counter = 0
        //count members of each category
        FileOccsCategoriesSource.fromFile(topicFile, new File(pathToArtCat)).foreach {
          case (resourceOcc, categories) => {
            counter += 1

            filteredCats = categories.filter(category => topicCats.contains(category))
            filteredCats.foreach(category => {
              if (!memberMap.contains(category))
                memberMap += (category -> 0)
              memberMap(category) += 1.0 / filteredCats.size
            })

            if (counter % 100000 == 0)
              LOG.info(counter + " occs processed")

          }
        }

        LOG.info("======================= Extract " + corpusSize + " occs to corpus =======================")
        val ratio = corpusSize.toDouble / memberMap.foldLeft(0.0)(_ + _._2)
        memberMap = memberMap.transform {
          (category, value) => value * ratio
        }
        counter = 0

        var written = false

        breakable {
          FileOccsCategoriesSource.fromFile(topicFile, new File(pathToArtCat)).foreach {
            case (occ, categories) => {
              if (counter < corpusSize) {
                written = false

                filteredCats = categories.filter(category => topicCats.contains(category))
                filteredCats.foreach(category => {
                  if (memberMap(category) > 0.0) {
                    if (!written) {
                      outputWriter.println(topic.getName + "\t" + occ.context.text)
                      written = true
                      counter += 1
                      if (counter % 10000 == 0)
                        LOG.info(counter + " examples written")
                    }

                    memberMap(category) -= 1 / filteredCats.size
                  }

                })
              }
              else
                break()

            }
          }
        }

        outputWriter.flush()
      }
    })

    outputWriter.close()
  }

  def generateCorpus(pathToSplittedOccs: String, nrOfExamples: Int, outputPath: String) {
    val outputWriter = new PrintWriter(new FileWriter(outputPath))

    val parentFile = new File(pathToSplittedOccs)

    var corpusSize = 0
    var sizes = Map[Topic, Int]()

    corpusSize = Int.MaxValue
    parentFile.listFiles().foreach(topicFile => {
      val topic = new Topic(topicFile.getName.substring(0, topicFile.getName.length - 4))
      var lineNr = 0
      Source.fromFile(topicFile).getLines().foreach(_ => lineNr += 1)
      sizes += (topic -> lineNr)
      corpusSize = math.min(lineNr, corpusSize)
    })

    if (nrOfExamples > 1 && nrOfExamples < corpusSize)
      corpusSize = nrOfExamples

    LOG.info("Writing corpus with size " + corpusSize + " for each topic")

    parentFile.listFiles().foreach(topicFile => {
      val topic = new Topic(topicFile.getName.substring(0, topicFile.getName.length - 4))

      LOG.info("======================= Processing " + topicFile.getName + " =======================")

      if (!topic.getName.equals(TopicUtil.CATCH_TOPIC)) {
        LOG.info("======================= Extract " + corpusSize + " occs to corpus =======================")
        val threshold = corpusSize.toDouble / sizes(topic)
        var counter = 0

        breakable {
          FileOccurrenceSource.fromFile(topicFile).foreach(occ => {
            if (counter < corpusSize) {
              if (math.random <= threshold) {
                outputWriter.println(topic.getName + "\t" + occ.context.text)
                counter += 1
                if (counter % 10000 == 0)
                  LOG.info(counter + " examples written")
              }
            }
            else
              break()

          })
        }

        outputWriter.flush()
      }
    })

    outputWriter.close()
  }

  private class CategoryTuple(var parent: DBpediaCategory,
                              var children: Set[DBpediaCategory],
                              var full: Boolean,
                              var nrPages: Double,
                              var members: Double)

}