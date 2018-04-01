package com.z.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}
/**
  * Created by root on 18-4-1.
  */
object RedisClient extends Serializable{
  private var redisHost: String= ""
  private var redisPort: Int = 0

  def setParam(redisHost: String, redisPort: Int) = {
    this.redisHost = redisHost
    this.redisPort = redisPort
  }

  def getResource: Jedis = pool.getResource
  private val redisTimeout = 30000
  private lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)
  private lazy val hook = new Thread(
    override def run() = {
      pool.destroy();
    }
  )
  sys.addShutdownHook(hook.run())
}
