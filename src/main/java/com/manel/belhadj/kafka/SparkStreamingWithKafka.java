package com.manel.belhadj.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.annotation.JsonFormat.Value;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;
/**
 *
 * @author manel
 *
 */
public class SparkStreamingWithKafka {
    public static void main(String[] argv) throws IOException, InterruptedException{

        // Configure Spark to connect to Kafka running on local machine
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"manel-pc:6667");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"grp8");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        //Configure Spark to listen messages in topic test
        Collection<String> topics = Arrays.asList("Test_Topic");

        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("SparkKafka10WordCount");

        //Read messages in batch of 30 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Start reading messages from Kafka and get DStream
         JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), 
		        ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));
         stream.window(Durations.seconds(30));
         

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(kafkaRecord -> kafkaRecord.value());
        // split suivant //t
        JavaDStream<List<String>> splitted = lines.map(x -> Arrays.asList(x.split("//t")) );
        
        JavaPairDStream<String, Integer> ip = splitted.mapToPair(w -> { 
        	 String IPs = w.get(4);
        	  Integer valuer = 1;
        	 return (new Tuple2<String, Integer >(IPs, valuer));
        } );
      
        JavaPairDStream<String, Integer> occ = ip.reduceByKey((v1,v2) -> v1+v2);
        occ.print();
        
        jssc.start();
        jssc.awaitTermination();
    }
}