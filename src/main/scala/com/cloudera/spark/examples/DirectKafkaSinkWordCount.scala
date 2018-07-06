/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.examples

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object DirectKafkaSinkWordCount {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaSinkWordCount <source-server> <bootstrap-servers> <protocol> <topic>
                            |  <source-server> The host/IP of the socket data source.
                            |  <bootstrap-servers> The Kafka "bootstrap.servers" configuration.
                            |  A comma-separated list of host:port.
                            |  <protocol> Protocol used to communicate with brokers.
                            |  Valid values are: 'PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'.
                            |  <topic> Single topic where output will be written.
                            |
      """.stripMargin)
      System.exit(1)
    }

    val Array(sourceServer, bootstrapServers, protocol, topic) = args

    val isUsingSsl = protocol.endsWith("SSL")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaSinkWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val commonParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "security.protocol" -> protocol,
      "sasl.kerberos.service.name" -> "kafka",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "group.id" -> "default"
    )

    val additionalSslParams = if (isUsingSsl) {
      Map(
        "ssl.truststore.location" -> "/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks",
        "ssl.truststore.password" -> "cloudera"
      )
    } else {
      Map.empty
    }

    val kafkaParams = commonParams ++ additionalSslParams

    import scala.collection.JavaConverters._

    // Get the lines, split them into words, count the words and send
    val lines = ssc.socketTextStream(sourceServer, 9999)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)

    // This part is really tricky and could be slow and/or error prone.
    // This is only for demonstration purpose and not the fastest solution (connection pool is missing)!
    // Please read this to reach maximum performance:
    // https://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    wordCounts.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val producer = new KafkaProducer[String, String](kafkaParams.asJava)
        partitionOfRecords.foreach { record =>
          val producerRecord = new ProducerRecord[String, String](topic, record._1, record._2.toString)
          producer.send(producerRecord)
        }
      })
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
