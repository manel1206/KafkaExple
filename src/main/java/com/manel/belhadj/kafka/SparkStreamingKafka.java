package com.manel.belhadj.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class SparkStreamingKafka {

	
	public static void main(String[] argv) throws Exception{

	        // Configure Spark to connect to Kafka running on local machine
	        Map<String, Object> kafkaParams = new HashMap<>();
	        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"omran-pc:6667");
	        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
	        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
	        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
	        //        kafkaParams.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

	        //Configure Spark to listen messages in topic test
//	        Collection<String> topics = Arrays.asList("Spark_Kafka");
	        Collection<String> topics = Arrays.asList("Spark_Kafka", "Spark_Kafka_Two");

	        SparkConf conf = new SparkConf()
	                .setMaster("local[*]")
	                .setAppName("SparkKafka10WordCount");

	        //Read messages in batch of 30 seconds
	        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

	        // Start reading messages from Kafka and get DStream
	        final JavaInputDStream<ConsumerRecord<String, String>> stream =
	                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), 
	                        ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

	        // Read value of each message from Kafka and return it
	        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
	            @Override
	            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
	                return kafkaRecord.value();
	            }
	        });

	        // Break every message into words and return list of words
	        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	            @Override
	            public Iterator<String> call(String line) throws Exception {
	                return Arrays.asList(line.split(" ")).iterator();
	            }
	        });
	            
	        // Take every word and return Tuple with (word,1)
	        JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
	            @Override
	            public Tuple2<String, Integer> call(String word) throws Exception {
	                return new Tuple2<>(word,1);
	            }
	        });

	        // Count occurance of each word
	        JavaPairDStream<String,Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
	            @Override
	            public Integer call(Integer first, Integer second) throws Exception {
	                return first+second;
	            }
	        });

	        //Print the word count
	        wordCount.print();

	        jssc.start();
	        jssc.awaitTermination();
	    }
}
