package com.z.ol.app.rec

import java.text.SimpleDateFormat
import java.util

import scala.collection.Map

/**
  * Created by root on 18-4-1.
  */
object ProcessRecommendNew {
  private val HOUR_SECONDS = 60 * 60

  private val DT_DIR_FORMAT = new SimpleDateFormat("yyyy-MM-dd/HH_mm_ss")

  def dstreamPrepareDataThenRecommend(dsUser: DStream[String], redisHost: String, redisPort: Int,
                                      recommender: DynamicFeatureSolution, modelKey: String,
                                      sc: SparkContext, nForTopLimit: Int, nForCandidateLimit: Int,
                                      data_update_interval_ms: Long, outputBase: String, resultKeyTemplate: String, expireHours: Int, reclogKeyTemplate: String,
                                      tag: String = null): Unit = {
    var last_data_update_ms: Long = 0

    val numExecutors = sc.getConf.getInt("spark.executor.instances", -1)
    assume(numExecutors != -1)

    dsUser.foreachRDD((rddUser, time) => {
      val batchPath = if (tag == null) {
        s"$outputBase/${DT_DIR_FORMAT.format(time.milliseconds)}"
      } else {
        s"$outputBase/${DT_DIR_FORMAT.format(time.milliseconds)}/$tag"
      }
      recommender.setInfoStoreDir(batchPath)

      if (time.milliseconds - last_data_update_ms >= data_update_interval_ms) {
        recommender.prepareDependency(time.milliseconds, sc)
        last_data_update_ms = time.milliseconds
      }

      rddUser.persist(StorageLevel.MEMORY_AND_DISK)
      recommend(rddUser, redisHost, redisPort, recommender, modelKey, sc, nForTopLimit, nForCandidateLimit, batchPath, time, resultKeyTemplate, expireHours, reclogKeyTemplate)
      rddUser.unpersist(blocking = false)
    })
  }

  // Prepare
  def recommend(actionUser: RDD[String], redisHost: String, redisPort: Int,
                recommender: DynamicFeatureSolution, modelKey: String,
                sc: SparkContext, nForTopLimit: Int, nForCandidateLimit: Int,
                batchPath: String, time: Time, resultKeyTemplate: String, expireHours: Int, reclogKeyTemplate: String): Unit = {
    val user2recLog: Map[String, Set[AnyRef]] = collectUsersRecLog(actionUser, redisHost, redisPort, reclogKeyTemplate)

    val modelPath: String = fetchModelPath(redisHost, redisPort, modelKey)
    Storage.saveSeq(Array(
      s"model_path = $modelPath",
      s"#user = ${actionUser.count()}"
    ), sc, s"$batchPath/info")

    recommender.init(sc, modelPath, no_weight_change = true)

    recommend(actionUser, user2recLog, redisHost, redisPort, recommender, nForTopLimit, nForCandidateLimit,
      sc, batchPath, time, resultKeyTemplate, expireHours)
  }

  def collectUsersRecLog(actionUser: RDD[String], redisHost: String, redisPort: Int, reclogKeyTemplate: String): collection.Map[String, Set[AnyRef]] = {
    val users2recLog = actionUser /*.repartition(numExecutors)*/ .mapPartitions(data => {
      RedisClient.setParam(redisHost, redisPort)
      val result = data.map(user => {
        val jedis = RedisClient.getResource
        val userRecLogKey = reclogKeyTemplate.format(user)
        val r: util.Set[String] = jedis.smembers(userRecLogKey)
        jedis.close()
        val userRecLog = r.toArray.toSet
        (user, userRecLog)
      })
      result
    }).collectAsMap()
    users2recLog
  }

  def fetchModelPath(redisHost: String, redisPort: Int, modelKey: String): String = {
    RedisClient.setParam(redisHost, redisPort)
    val jedis = RedisClient.getResource
    val modelPath = jedis.lrange(modelKey, 0, 0).get(0)
    jedis.close()
    modelPath
  }

  private def recommend(actionUser: RDD[String], users2recLog: Map[String, Set[AnyRef]], redisHost: String, redisPort: Int,
                        recommender: DynamicFeatureSolution, nForTopLimit: Int, nForCandidateLimit: Int,
                        sc: SparkContext, batchPath: String, time: Time, resultKeyTemplate: String, expireHours: Int): Unit = {
    // Rank core
    val rddUserRankedItems = recommender.rankDocsForUsersWithFeature(actionUser, nForCandidateLimit, sc)

    // Filtering
    val num_executors = Utils.getNumExecutors(sc)
    val rddUserRec = rddUserRankedItems.coalesce(num_executors).map(userDocs => {
      val user = userDocs._1
      val recLog = users2recLog.getOrElse(user, Set[String]())
      val rec = userDocs._2
        .filter(item_score => !recLog.contains(item_score._1))
        .toArray.sortBy(_._2).reverse
        .take(nForTopLimit)
      (user, rec)
    })
      .repartition(Utils.getNumExecutors(sc))
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Write to redis
    rddUserRec.foreachPartition(partition => {
      RedisClient.setParam(redisHost, redisPort)
      var failed_count: Int = 0
      partition.foreach(userRecList => {
        val userKey = resultKeyTemplate.format(userRecList._1)
        try {
          val jedis = RedisClient.getResource
          jedis.del(userKey)
          userRecList._2.foreach(item_score => {
            jedis.zadd(userKey, item_score._2, item_score._1)
            jedis.expire(userKey, HOUR_SECONDS * expireHours)
          })
          jedis.close()
        } catch {
          case exception: redis.clients.jedis.exceptions.JedisConnectionException =>
            println("JEDIS FAILURE JedisConnectionException: " + userRecList._1)
            failed_count += 1
            if (failed_count >= 10) {
              println("JEDIS FAILURE 10 times: " + userRecList._1)
              throw exception
            } else {
              Thread.sleep(1000)
            }
          case exception: java.net.SocketException =>
            println("JEDIS FAILURE SocketException: " + userRecList._1)
            failed_count += 1
            if (failed_count >= 10) {
              println("JEDIS FAILURE 10 times: " + userRecList._1)
              throw exception
            }
        }
      })
    })

    // Logging
    actionUser.coalesce(1).saveAsTextFile(s"$batchPath/users")

    val modelWeights = recommender.getLatestRankingWeights
    rddUserRec.map(
      ur => ur._1 + "\n" + ur._2.map(
        t => s"${t._1}\t${t._2.toFloat}\t${Utils.featureHitWeightsString(t._3, modelWeights)}"
      ).mkString("\n")
    )
      .coalesce(1)
      .saveAsTextFile(s"$batchPath/rec_feature")
    //    val sample = rddUserRec.first
    //    Storage.saveStrings(
    //      Array(sample._1) ++ sample._2.map(
    //        t=>s"${t._1}\t${t._2.toFloat}\t${Utils.featureHitWeightsString(t._3, modelWeights)}"
    //      ),
    //      sc,
    //      s"$batchPath/rec_feature_sample"
    //    )

    rddUserRec.unpersist(blocking = false)
  }
}
