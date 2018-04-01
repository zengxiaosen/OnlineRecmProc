package com.z.ol.process

/**
  * Created by root on 18-4-1.
  */
object ProcessStream {
  val format = new java.text.SimpleDateFormat("yyyy-MM-dd/HH_mm_ss")

  def createKafkaInstanceDStream(ssc: StreamingContext, kafkaParams: Map[String, String],
                                 imp_topic: String, click_topic: String, lang: String,
                                 output_dir: String)
  : DStream[(String, String, Double)] = {
    val userClickStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(click_topic))
    val userRefreshStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(imp_topic))

    val event_ds: DStream[(String, String)] = userRefreshStream.union(userClickStream)
    //    if (output_dir != null) {
    //      event_ds.foreachRDD((event_rdd,time) => {
    //        val dt = format.format(new Date(time.milliseconds))
    //        event_rdd.repartition(1).saveAsTextFile(s"$output_dir/$dt/data/event")
    //      })
    //    }

    val instance_ds = event_ds.transform(event_rdd => {
      Input.parseInstance(event_rdd.map(_._2), lang)
    })
    if (output_dir != null) {
      instance_ds.foreachRDD((instance_rdd, time) => {
        val dt = format.format(new Date(time.milliseconds))
        instance_rdd.coalesce(1).saveAsTextFile(s"$output_dir/$dt/data/instance")
      })
    }
    instance_ds
  }
}
