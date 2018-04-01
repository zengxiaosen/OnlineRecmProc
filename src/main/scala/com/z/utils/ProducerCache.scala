package com.z.utils

import java.util.Properties

import java.util.Properties
import scala.collection.mutable
import kafka.producer.{ProducerConfig, Producer}

/**
  * Created by root on 18-4-1.
  */
object ProducerCache {

  private val producers = new mutable.HashMap[Properties, Any]()
  def getProducer[K,V](config: Properties): Producer[K, V] = {
    producers.getOrElse(config, {
      val producer = new Producer[K, V](new ProducerConfig(config))
      producers(config) = producer
      producer
    }).asInstanceOf[Producer[K,v]]
  }

}
