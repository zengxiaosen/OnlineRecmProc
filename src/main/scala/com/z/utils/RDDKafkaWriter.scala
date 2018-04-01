package com.z.utils

/**
  * Created by root on 18-4-1.
  */
import java.util.Properties
import scala.reflect.ClassTag
import kafka.producer.{KeyedMessage, Producer}
import org.apache.spark.rdd.RDD
class RDDKafkaWriter[T: ClassTag](@transient rdd: RDD[T]) extends KafkaWriter[T] {
  override def writeToKafka[K, V](producerConfig: Properties, serializerFunc: (T) => KeyedMessage[K, V]): Unit = {
    rdd.foreachPartition(events => {
      val producer : Producer[K, V] = ProducerCache.getProducer(producerConfig)
      producer.send(events.map(serializerFunc).toArray: _*)
    })
  }

}
