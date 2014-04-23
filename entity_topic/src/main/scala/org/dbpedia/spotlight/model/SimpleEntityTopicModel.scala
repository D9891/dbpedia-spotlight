package org.dbpedia.spotlight.model

import org.dbpedia.spotlight.db.memory.MemoryCandidateMapStore
import scala.collection.mutable
import org.dbpedia.spotlight.io.EntityTopicDocument
import org.dbpedia.spotlight.storage.CountStore
import scala.util.Random

/**
 * @author dirk
 *          Date: 4/23/14
 *          Time: 12:16 PM
 */
class SimpleEntityTopicModel(numTopics:Int, numEntities:Int, vocabularySize:Int, numMentions:Int, candMap:MemoryCandidateMapStore,
                             alpha: Double, beta: Double, gamma: Double, delta: Double) {

    val entityTopicMatrix = Array.ofDim[Int](numTopics,numEntities)
    val sparseMentionEntityMatrix = Array.tabulate(candMap.candidates.size)(i => {
        val candCounts = candMap.candidateCounts(i)
        candMap.candidates(i).zip(candCounts.map(candMap.qc)).foldLeft(mutable.Map[Int,Int]())((acc,p) => {acc += p; acc})
    })
    val sparseWordEntityMatrix = Array.fill(vocabularySize)(mutable.HashMap[Int,Int]())
    val topicCounts = new Array[Int](numTopics)
    val entityCounts = new Array[Int](numEntities)
    val assignmentCounts = new Array[Int](numEntities)

    def gibbsSampleDocument(doc: EntityTopicDocument, withUpdate:Boolean = false, init:Boolean = false) {
        sampleNewTopics(doc,withUpdate,init)
        sampleNewEntities(doc,withUpdate,init)
        sampleNewAssignments(doc,withUpdate,init)
    }

    private[model] def sampleNewTopics(doc: EntityTopicDocument, withUpdate:Boolean, init: Boolean) = {
        val docTopicCounts = doc.entityTopics.foldLeft(mutable.Map[Int, Int]())((acc, topic) => {
            if (topic >= 0)
                acc += (topic -> (acc.getOrElse(topic, 0) + 1))
            acc
        })

        (0 until doc.entityTopics.length).foreach(idx => {
            val entity = doc.mentionEntities(idx)
            val oldTopic = doc.entityTopics(idx)

            val newTopic = sampleFromProportionals(topic => {
                val add = {
                    if (topic == oldTopic) -1 else 0
                }
                (docTopicCounts.getOrElse(topic, 0) + add + alpha) *
                    (entityTopicMatrix(topic)(entity) + add + beta) / (topicCounts(topic) + add + numTopics * beta)
            }, 0 until numTopics)

            doc.entityTopics(idx) = newTopic

            if(withUpdate) {
                if (!init && oldTopic >= 0) {
                    topicCounts(oldTopic)-=1
                    entityTopicMatrix(oldTopic)(entity)-=1
                }
                topicCounts(newTopic)+=1
                entityTopicMatrix(newTopic)(entity)+=1
            }
        })
    }

    private[model] def sampleNewEntities(doc: EntityTopicDocument, withUpdate:Boolean, init:Boolean) = {
        //Sample new entities
        val docEntityCounts = doc.mentionEntities.foldLeft(mutable.Map[Int, Int]())((acc, entity) => {
            if (entity >= 0)
                acc += (entity -> (acc.getOrElse(entity, 0) + 1))
            acc
        })
        val docAssignmentCounts = doc.tokenEntities.foldLeft(mutable.Map[Int, Int]())((acc, entity) => {
            if (entity >= 0)
                acc += (entity -> (acc.getOrElse(entity, 0) + 1))
            acc
        })
        (0 until doc.mentionEntities.length).foreach(idx => {
            val oldEntity = doc.mentionEntities(idx)
            val mention = doc.mentions(idx)
            val topic = doc.entityTopics(idx)
            val mentionCounts = sparseMentionEntityMatrix(mention)

            //Keep original assignments in initialization phase
            val newEntity = if (init && oldEntity >= 0)
                oldEntity
            else {
                val entityTopicCounts = entityTopicMatrix(topic)
                val cands = candMap.candidates(mention)

                sampleFromProportionals(entity => {
                    val add = {
                        if (entity == oldEntity) -1 else 0
                    }
                    val cte = entityTopicCounts(entity)
                    val cem = mentionCounts(entity)
                    val ce = entityCounts(entity)

                    (cte + add + beta) *
                        (cem + add + gamma) / (ce + add + numMentions * gamma) *
                        (0 until docAssignmentCounts.getOrElse(entity, 0)).foldLeft(1)((acc, _) => acc * (docEntityCounts(entity) + 1) / docEntityCounts(entity))
                }, cands)
            }

            doc.mentionEntities(idx) = newEntity

            if(withUpdate) {
                if(!init && oldEntity >= 0) {
                    entityTopicMatrix(topic)(oldEntity) -= 1
                    mentionCounts(oldEntity) = mentionCounts(oldEntity) - 1
                    entityCounts(oldEntity) = entityCounts(oldEntity)-1
                }

                //It is possible, that no entity i found for that mention
                if (newEntity >= 0) {
                    entityTopicMatrix(topic)(newEntity) += 1
                    //Initialized with anchor counts, so no updates if oldEntity was an anchor and we are in init phase
                    if(!init || oldEntity < 0)
                        mentionCounts(newEntity) = mentionCounts(newEntity)+ 1
                    entityCounts(newEntity) = entityCounts(newEntity) + 1
                }
            }
        })
    }

    private[model] def sampleNewAssignments(doc: EntityTopicDocument, withUpdate:Boolean, init: Boolean) = {
        //Sample new assignments
        val docEntityCounts = doc.mentionEntities.foldLeft(mutable.Map[Int, Int]())((acc, entity) => {
            if (entity >= 0)
                acc += (entity -> (acc.getOrElse(entity, 0) + 1))
            acc
        })
        val candidateEntities = docEntityCounts.keySet
        (0 until doc.tokenEntities.length).foreach(idx => {
            val oldEntity = doc.tokenEntities(idx)
            val token = doc.tokens(idx)
            val entityTokenCounts = sparseWordEntityMatrix(token)

            val newEntity = sampleFromProportionals(entity => {
                val add = {
                    if (entity == oldEntity) -1 else 0
                }
                docEntityCounts(entity) *
                    (entityTokenCounts.getOrElse(entity,0) + add + delta) / (assignmentCounts(entity) + add + vocabularySize * delta)
            }, candidateEntities)

            doc.tokenEntities(idx) = newEntity

            if(withUpdate) {
                if (!init && oldEntity >= 0) {
                    val oldCount = entityTokenCounts(oldEntity)
                    if(oldCount == 0)
                        entityTokenCounts.remove(oldEntity)
                    else
                        entityTokenCounts(oldEntity) = oldCount - 1
                    assignmentCounts(oldEntity) -= 1
                }

                entityTokenCounts += newEntity -> (1 + entityTokenCounts.getOrElse(newEntity,0))
                assignmentCounts(newEntity) += 1
            }
        })
    }

    private[model] def sampleFromProportionals(calcProportion:Int => Double, candidates:Traversable[Int]) = {
        var sum = 0.0
        val cands = candidates.map(cand => {
            var res = calcProportion(cand)
            sum += res
            (cand, res)
        })
        var random = Random.nextDouble() * sum
        var selected = (Int.MinValue, 0.0)
        val it = cands.toIterator
        while (random >= 0.0 && it.hasNext) {
            selected = it.next()
            random -= selected._2
        }
        selected._1
    }

}
