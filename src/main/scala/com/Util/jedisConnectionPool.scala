package com.Util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)
  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"192.168.235.130",6379,1000,"123456")

  def getConnection():Jedis ={
    pool.getResource
  }
}
