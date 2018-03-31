package com.adatafun.dp.service;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * StreamingProcessingFromTCPSocket.java
 * Copyright(C) 2017 杭州风数科技有限公司
 * Created by wzt on 2018/3/26.
 */
public class StreamingProcessingFromTCPSocket {

    public static void main(String[] args) throws Exception {
        //Create a local StreamingContex with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TCP socket source");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        //Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("192.168.1.131", 9998);

        //Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        //Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        jssc.start(); //Start the computation
        jssc.awaitTermination();
    }

}
