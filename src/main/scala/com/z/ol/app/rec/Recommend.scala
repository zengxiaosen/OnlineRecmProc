package com.z.ol.app.rec

/**
  * Created by root on 18-4-1.
  */
object Recommend {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(args(0).toInt))

    val brokers = args(1)
    val kafkaParams = Map("metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")
    val userActionStream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(args(2)))

    val mongoInputSplitSize = args(3)
    val data_update_interval_ms = args(4).toInt
    val docProfileMongoInputUri = args(5)

    val nForTopLimit = args(6).toInt
    val candidateTop = args(7).toInt

    val redisHost = args(8)
    val redisPort = args(9).toInt
    val modelKey = args(10)

    val outputBase = args(11)

    val userFilter = args(12)

    val resultKeyTemplate = args(13)
    val expireHours = args(14).toInt

    val reclogKeyTemplate = args(15)

    val entityMultiplier = args(16).toDouble

    //
    val dsUser = userActionStream.transform((userJson, time) => {
      userJson.map(line => {
        val userJson = Event.parseUserJson(line._2)
        (userJson.user_seq_id.toString, userJson.version, userJson.custom_categories)
      })
        .filter(t => t._2 == userFilter || t._2 == null)
        .map(_._1)
        .distinct()
    })

    //
    val recommender = new SolutionNgCategoryEntityTfIdfLiteMix(docProfileMongoInputUri, mongoInputSplitSize, entityMultiplier, modeOnline = true)

    ProcessRecommendNew.dstreamPrepareDataThenRecommend(dsUser, redisHost, redisPort,
      recommender, modelKey, ssc.sparkContext, nForTopLimit, candidateTop, data_update_interval_ms,
      outputBase, resultKeyTemplate, expireHours, reclogKeyTemplate)

    ssc.start()
    ssc.awaitTermination()
  }
}
