package com.z.utils

import java.util.Properties

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import kafka.producer.KeyedMessage
/**
  * Created by root on 18-4-1.
  */
object KafkaWriter {
  implicit def createKafkaOutputWriter[T: ClassTag, K, V](rdd: RDD[T]): KafkaWriter[T] = {
    new RDDKafkaWriter[T](rdd)
  }
}

abstract class KafkaWriter[T: ClassTag](){
  def writeToKafka[K, V](producerConfig: Properties, serializerFunc: T => KeyedMessage[K, V]): Unit
}
