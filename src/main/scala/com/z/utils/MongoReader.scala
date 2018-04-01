package com.z.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.BSONObject
/**
  * Created by root on 18-4-1.
  */
class MongoReader (inputUri: String, sc: SparkContext){
  def initRDD(): RDD[(Object, BSONObject)] = {
    val hadoopConf = new Configuration()
    hadoopConf.set("mongo.input.uri", inputUri)
    sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])

  }

}

class AuthedMongoReader(authUri: String, inputUri: String, sc: SparkContext) extends MongoReader(inputUri: String, sc: SparkContext)
{
  override def initRDD(): RDD[(Object, BSONObject)] = {
    val hadoopConf = new Configuration();
    hadoopConf.set("mongo.auth.uri", authUri)
    hadoopConf.set("mongo.input.uri", inputUri)
    sc.newAPIHadoopRDD(hadoopConf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])
  }
}

class ConfiguredMongoReader(sc: SparkContext, conf: Configuration){
  def initRDD(): RDD[(Object, BSONObject)] = {
    sc.newAPIHadoopRDD(conf, classOf[com.mongodb.hadoop.MongoInputFormat], classOf[Object],
      classOf[BSONObject])
  }
}