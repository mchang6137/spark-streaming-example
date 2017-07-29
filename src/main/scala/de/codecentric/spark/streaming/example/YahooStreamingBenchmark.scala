/*
 * Copyright 2016 Matthias Niehoff
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package de.codecentric.spark.streaming.example

import java.io.File
import java.io.PrintWriter
import java.util
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ Duration, Milliseconds, Seconds, Time }

import kafka.serializer.StringDecoder
import org.apache.spark.streaming
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream
import org.apache.spark.SparkConf
import org.json.JSONObject
import org.sedis._
import redis.clients.jedis._
import scala.collection.Iterator
import org.apache.spark.rdd.RDD
import java.util.{ UUID, LinkedHashMap }
import compat.Platform.currentTime
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

object YahooStreamingBenchmark {

  def getTimeDiff(cwc_iter: Iterator[((String, Long), Int)]): Iterator[(String, Long, Long)] = {
    if (cwc_iter.hasNext) {
      // val firstCWC = cwc_iter.next
      // logInfo("Starting reduce task at " + System.currentTimeMillis +
      //  " first window ts " + firstCWC._1._2)
      val out = cwc_iter.map { cw =>
        val campaign = cw._1._1
        val window_ts = cw._1._2
        val curT = System.currentTimeMillis
        val diff = (curT - window_ts)
        (campaign, window_ts, diff)
      }
      out
    } else {
      Iterator.empty
    }
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: KafkaStreamingBillboard <brokers> <topics> <cassandraHost>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <cassandraHost> is a list of one ore more Cassandra nodes
           |  <batchSize> is the size of the batch 
        """.stripMargin
      )
      System.exit(1)
    }

    val brokers = args(0)
    val topics = args(1)
    val redis_host = args(2)
    val batch_size = args(3).toLong

    val time_divisor: Long = 10000L

    val sparkConf = new SparkConf().setAppName("YahooBenchmarkTbot")
    sparkConf.remove("spark.jars")

    val ssc = new StreamingContext(sparkConf, Milliseconds(batch_size))

    println("michael Spark streaming context started")
    val start = System.nanoTime()

    //Read from Kafka
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, , "auto.offset.reset" -> "smallest")
    System.err.println("Trying to connect to Kafka at " + brokers)
    val kafka_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    System.err.println("michael!")
    kafka_stream.print()

    val kafkaRawData = kafka_stream.map(_._2)
    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson(_))

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(_(4).equals("view"))

    //project the event, basically filter the fileds.
    val projected = filteredOnView.map(eventProjection(_))

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined = projected.mapPartitions(queryRedisTopLevel(_, redis_host), false)

    val campaign_timeStamp = redisJoined.map(campaignTime(_))
    //each record in the RDD: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
    //DStream[((String,Long),String)]

    // since we're just counting use reduceByKey
    val totalEventsPerCampaignTime = campaign_timeStamp.mapValues(_ => 1).reduceByKey(_ + _)

    //DStream[((String,Long), Int)]
    //each record: key:(campaign_id, window_time),  Value: number of events

    //Repartition here if desired to use more or less executors
    //    val totalEventsPerCampaignTime_repartitioned = totalEventsPerCampaignTime.repartition(20)

    totalEventsPerCampaignTime.foreachRDD { rdd =>
      rdd.foreachPartition(writeRedisTopLevel(_, redis_host))
    }

    // Start the computation
    ssc.start
    ssc.awaitTermination

    val end = System.nanoTime()
    println("Time for all results is " + (end - start) / 1e9 + " secs")
  }

  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder();
    hosts.foreach({
      if (!joined.isEmpty) {
        joined.append(",");
      }

      joined.append(_).append(":").append(port);
    })
    return joined.toString();
  }

  def parseJson(jsonString: String): Array[String] = {
    val parser = new JSONObject(jsonString)
    Array(
      parser.getString("user_id"),
      parser.getString("page_id"),
      parser.getString("ad_id"),
      parser.getString("ad_type"),
      parser.getString("event_type"),
      parser.getString("event_time"),
      parser.getString("ip_address")
    )
  }

  def eventProjection(event: Array[String]): Array[String] = {
    Array(
      event(2), //ad_id
      event(5)
    ) //event_time
  }

  def queryRedisTopLevel(eventsIterator: Iterator[Array[String]], redisHost: String): Iterator[Array[String]] = {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
    var ad_to_campaign = new util.HashMap[String, String]();
    val eventsIteratorMap = eventsIterator.map(event => queryRedis(pool, ad_to_campaign, event))
    pool.underlying.getResource.close
    return eventsIteratorMap
  }

  def queryRedis(pool: Pool, ad_to_campaign: util.HashMap[String, String], event: Array[String]): Array[String] = {
    val ad_id = event(0)
    val campaign_id_cache = ad_to_campaign.get(ad_id)
    if (campaign_id_cache == null) {
      pool.withJedisClient { client =>
        val campaign_id_temp = Dress.up(client).get(ad_id)
        if (campaign_id_temp != None) {
          val campaign_id = campaign_id_temp.get
          ad_to_campaign.put(ad_id, campaign_id)
          Array(campaign_id, event(0), event(1))
          //campaign_id, ad_id, event_time
        } else {
          Array("Campaign_ID not found in either cache nore Redis for the given ad_id!", event(0), event(1))
        }
      }
    } else {
      Array(campaign_id_cache, event(0), event(1))
    }
  }

  def campaignTime(event: Array[String]): ((String, Long), String) = {
    val time_divisor: Long = 10000L
    ((event(0), time_divisor * (event(2).toLong / time_divisor)), event(1))
    //Key: (campaign_id, window_time),  Value: ad_id
  }

  def writeRedisTopLevel(campaign_window_counts_Iterator: Iterator[((String, Long), Int)], redisHost: String) {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))

    campaign_window_counts_Iterator.foreach(campaign_window_counts => writeWindow(pool, campaign_window_counts))

    pool.underlying.getResource.close
  }

  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), Int)): String = {
    val campaign_window_pair = campaign_window_counts._1
    val campaign = campaign_window_pair._1
    val window_timestamp = campaign_window_pair._2.toString
    val window_seenCount = campaign_window_counts._2
    pool.withJedisClient { client =>

      val dressUp = Dress.up(client)
      var windowUUID = dressUp.hmget(campaign, window_timestamp)(0)
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString
        dressUp.hset(campaign, window_timestamp, windowUUID)
        var windowListUUID: String = dressUp.hmget(campaign, "windows")(0)
        if (windowListUUID == null) {
          windowListUUID = UUID.randomUUID.toString
          dressUp.hset(campaign, "windows", windowListUUID)
        }
        dressUp.lpush(windowListUUID, window_timestamp)
      }
      dressUp.hincrBy(windowUUID, "seen_count", window_seenCount)
      dressUp.hset(windowUUID, "time_updated", currentTime.toString)
      return window_seenCount.toString
    }

  }
}
