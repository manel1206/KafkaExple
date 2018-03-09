package com.manel.belhadj.kafka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.jute.compiler.generated.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDriver {

    private final static String FILE_PATH="hdfs://manel-pc:8020/user/admin/InputTSV/input.tsv";
    private final static String TOPIC_NAME="kafkaTopicThree"; //"kafkaTopicOne";
    private final static String KEY_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer";
    private final static String VALUE_SERIALIZER="org.apache.kafka.common.serialization.StringSerializer";
    private final static String HOST_PORT="manel-pc:6667";

    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, IOException, ParseException{
        meth("c", "r");
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,HOST_PORT);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,KEY_SERIALIZER);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,VALUE_SERIALIZER);
        
        //prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        //prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //KafkaProducer<String, String> producer = new KafkaProducer<String, String>(config);

        String currentLine = null;
        BufferedReader reader = new BufferedReader(new FileReader(FILE_PATH));
        
        while((currentLine = reader.readLine()) != null){
            System.out.println("---------------------");
            System.out.println(currentLine);
            System.out.println("---------------------");
            ProducerRecord<String, String>  message = new ProducerRecord<String, String>(TOPIC_NAME, currentLine);
            producer.send(message);
        }
        reader.close();
        producer.close();   
    }  
    
    private static void meth(String... oar) {
    	System.out.println(oar.length);
    }
}