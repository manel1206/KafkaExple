package com.manel.belhadj.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {

	private static final String TOPIC_NAME = "SparkStreaming";
	private final static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	private final static String HOST_PORT = "manel-pc:6667";

	public static void main(String[] args) {

		Properties props = new Properties();
		//long millis = System.currentTimeMillis() ;
		props.put("bootstrap.servers", HOST_PORT);
		props.put("group.id", "gr12"); // To change the group id
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", KEY_DESERIALIZER);
		props.put("value.deserializer", VALUE_DESERIALIZER);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	
		
		  KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		  //Kafka Consumer subscribes list of topics here.
	        consumer.subscribe(Arrays.asList(TOPIC_NAME));
	        //print the topic name
	        System.out.println("Subscribed to topic " + TOPIC_NAME);
	        while (true) {
	            ConsumerRecords<String, String> records = consumer.poll(100);
	            
	            for (ConsumerRecord<String, String> record : records)
	            {    
	            	Connection connection = new Connection();
	            	Date date = new Date();
	            	connection.setLocalDate(date);
	            	connection.setUrl(String.valueOf(record.offset()));
	            	
	                System.out.println("%%%%%%%");
	                
	                System.out.println(connection.toString(date, String.valueOf(record.offset())));
	               
	            }

	        }
	    }
	
	}


