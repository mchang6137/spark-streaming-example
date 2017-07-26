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

package de.codecentric.spark.streaming.example

import java.io.InputStream
import java.io.File
import java.io.PrintWriter
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer._
import java.util.HashMap

import scala.io.Source
import scala.util.Random

import net.minidev.json.JSONObject
import net.minidev.json.parser.JSONParser

/**
 *
 */
object KafkaEventPublisher{

  val NUM_PAGE_IDS = 100
  val NUM_USER_IDS = 100
  val NUM_CAMPAIGNS = 100
  val ADS_PER_CAMPAIGN = 10

  case class BenchmarkIds(
    userIds: Array[String],
    pageIds: Array[String],
    campaignIds: Array[String],
    eventTypes: Array[String],
    adTypes: Array[String],
    adIds: Array[String])

  def generateJson(
      ids: BenchmarkId,
      rnd: java.util.Random,
      sb: java.lang.StringBuilder,
      idx: Int,
      useCurrentTime: Boolean): (String, Long) = {
      
    val userIds = ids.value.userIds
    val pageIds = ids.value.pageIds
    val adTypes = ids.value.adTypes
    val eventTypes = ids.value.eventTypes
    val adIds = ids.value.adIds

    val adId = adIds(idx % adIds.length)

    // ad_type value is immediately discarded. The original generator would put a
    // string with 38/5 = 7.6 chars. We put 8.
    sb.setLength(0)
    sb.append("{\"user_id\":\"")
    sb.append(userIds(idx % NUM_USER_IDS))
    sb.append("\",\"page_id\":\"")
    sb.append(pageIds(idx % NUM_PAGE_IDS))
    sb.append("\",\"ad_id\":\"")
    sb.append(adId)
    sb.append("\",\"ad_type\":\"")
    sb.append("banner78")
    sb.append("\",\"event_type\":\"")
    sb.append(eventTypes(idx % eventTypes.length))
    sb.append("\",\"event_time\":\"")
    var retTime = startTime
    if (useCurrentTime) {
      retTime = System.currentTimeMillis
      sb.append(retTime)
    } else {
      val offset = rnd.nextInt(timeSlice)
      sb.append(retTime + offset)
    }
    sb.append("\",\"ip_address\":\"1.2.3.4\"}")
    (sb.toString(), retTime)
  }

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaEventPublisher <metadataBrokerList> <topic> <num_trials> <totalNumElems> <UseCurrentTime>")
      System.exit(1)
    }
    
    val brokers = args(0)
    val topic = args(1)
    val num_trials = args(2).toInt
    val num_elements = args(3).toInt
    val use_current_time = args(4).toBoolean

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](props)

    //Generate Some Random Advertising Messages for the Yahoo Benchmark
    val campaignIds = Array.fill(NUM_CAMPAIGNS)(java.util.UUID.randomUUID).map(_.toString)
    val pageIds = Array.fill(NUM_PAGE_IDS)(java.util.UUID.randomUUID).map(_.toString)
    val userIds = Array.fill(NUM_USER_IDS)(java.util.UUID.randomUUID).map(_.toString)
    val adTypes = Array("banner", "modal", "sponsored-search", "mail", "mobile")
    val eventTypes = Array("view", "click", "purchase")

    val adIdToCampaignMap = campaignToAdIds.flatMap { c =>
      c._2.map { a =>
        (a, c._1)
      }
    }.toMap

    val adIds = adIdToCampaignMap.keys.toArray
    val benchmarkIds = BenchmarkIds(userIds, pageIds, campaignIds, eventTypes, adTypes, adIds)

    for (a <- 1 until numTrials){
        var idx = 0     
        while (numElemsGenerated < total_num_elems) {
	    val stringBuilder = new java.lang.StringBuilder
            val advertise_result = generateJson(benchmarkIds, ThreadLocalRandom.current, stringBuilder, idx, use_current_time)
	    val message = new ProducerRecord[String, String](topic, null, advertise_result)
	    producer.send(message)
	    totalNumElems += 1
	}
	System.out.println("Sent " + total_num_elems + " events to Kafka")
    }	
  }
}
