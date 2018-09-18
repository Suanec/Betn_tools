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

// scalastyle:off println
package com.weibo.tools

import java.io.{BufferedInputStream,FileInputStream}
import java.net.URI
import java.io.BufferedInputStream
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.{Configuration => hdfsConfig}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import org.apache.spark.{SparkConf, SparkContext}

object Bandwidthlimited_local2HDFS_Writer {
  val kiloByte = 1024
  def upload_one_buffer(inStream : java.io.BufferedInputStream,
    outputStream : org.apache.hadoop.fs.FSDataOutputStream,
    log_buffer : Array[Byte],
    pre_buffer_sum : Long,
    totalSize : Long
  ) : Long = {
    val readSize = inStream.read(log_buffer) 
    val buffer_sum = pre_buffer_sum + readSize
    outputStream.write(log_buffer.splitAt(readSize)._1)
    outputStream.flush
    TimeUnit.MILLISECONDS.sleep(999)
    // println(s"${inStream} uploading. ${buffer_sum} uploaded. readSize : ${readSize}. ${buffer_sum * 100 / totalSize}% finished. ")
    buffer_sum
  }
  def LocalLog2HDFS_Writer(sc : SparkContext, 
    localSrcPath : String, 
    remoteTarPath : String,
    bandwidth : String
  ) : Long = {
    sc.hadoopConfiguration.setBoolean("dfs.support.append",true)
    val hdfs = FileSystem.get(new URI("/"), sc.hadoopConfiguration)
    val filePath = new Path(remoteTarPath)
    val inStream = new BufferedInputStream(new FileInputStream(localSrcPath))
    val totalSize = inStream.available
    hdfs.exists(filePath) match {
      case false => hdfs.create(filePath).close
      case true => println(hdfs.getFileStatus(filePath).toString)
    }
    val outputStream = hdfs.append(filePath)
    val buffer_size = kiloByte * bandwidth.toInt
    val log_buffer = new Array[Byte](buffer_size)
    var buffer_sum = 0L
    try {
        while(inStream.available >= buffer_size) {
          val readSize = inStream.read(log_buffer) 
          buffer_sum += readSize
          outputStream.write(log_buffer.splitAt(readSize)._1)
          outputStream.flush
          outputStream.hflush
          println(s"${localSrcPath} uploading. ${buffer_sum} uploaded. readSize : ${readSize}. ${buffer_sum * 100 / totalSize}% finished. ")
          TimeUnit.MILLISECONDS.sleep(999)
        }
        if(inStream.available > 0) {
          val readSize = inStream.read(log_buffer) 
          buffer_sum += readSize
          outputStream.write(log_buffer.splitAt(readSize)._1)
          outputStream.flush
          println(s"${localSrcPath} uploading. ${buffer_sum} uploaded. readSize : ${readSize}. ${buffer_sum * 100 / totalSize}% finished. ")
        }
      } finally {
        inStream.close
        outputStream.close
      }  
      buffer_sum
  }
  def Local2HDFS_Writer(sc : SparkContext, args: Array[String]) : Long = {
    val helper_info = """    the file localSrcPath pointed limited 1.999G
    Bandwidthlimited_local2HDFS_Writer localSrcPath remoteTarPath bandwidth=10K(by KB)"""
    println(helper_info)
    require(args.size >= 3, helper_info)
    val localSrcPath = args(0)
    val remoteTarPath = args(1)
    val bandwidth = args(2)
    LocalLog2HDFS_Writer(sc, localSrcPath, remoteTarPath, bandwidth)
  }
  def LocalLogReducer2HDFS(sc : SparkContext, taskList : List[(String, String)], bandwidth : String) : Int = {
    var sum = 0
    taskList.iterator.map{
      case (localSrcPath, remoteTarPath) =>
      LocalLog2HDFS_Writer(sc, localSrcPath, remoteTarPath, bandwidth) 
      sum += 1
    }
    sum
  }
  def LocalLogReducer(sc : SparkContext, srcParentPath : String, bandwidth : String) = {}

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Bandwidthlimited_local2HDFS_Writer")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    Local2HDFS_Writer(sc, args)
    sc.stop()
  }
}
