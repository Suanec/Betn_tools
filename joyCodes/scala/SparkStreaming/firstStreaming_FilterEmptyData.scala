import org.apache.spark.streaming._
val ssc = new StreamingContext(sc, Seconds(20))
val lines = ssc.socketTextStream("localhost", 9999)
lines.map(_.split(" ")).map(x => x -> 1).reduceByKey(_ + _).print
ssc.start

    val words = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).print()

ssc.awaitTermination

package org.apache.spark.examples.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import socket
skt = socket.socket()
skt.bind(("localhost",9999))
skt.listen(10)

for i in range(1,10):
