package com.manel.belhadj.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerApi {

	private final static String PATH = "/user/kafka-example/input.tsv";
	public static final String TOPIC_NAME = "manel_khouloud";
	public static final String HDFS_URI = "hdfs://192.168.1.113:8020";
	
	public static Properties Config() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "tcb1.server.hdp.com:6667");
		props.put("acks", "all");
		props.put("retries", 0);	
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

	public static Configuration HDFSConf() {

		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://192.168.1.113:8020");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		
		return conf;

	}

	public static void main(String[] args) throws Exception {

		Properties props = Config();
		Configuration conf = HDFSConf();
		
		String uri = "hdfs://192.168.1.113:8020";
        URI hdfsuri = URI.create(uri);
        
		FileSystem fs = FileSystem.get(hdfsuri, conf);
		Path newFolderPath = new Path(PATH);
		FSDataInputStream inputStream = fs.open(newFolderPath);

		InputStreamReader r = new InputStreamReader(inputStream);

		BufferedReader br = new BufferedReader(r);

		Producer<String, String> kafkaproducer = new KafkaProducer<String, String>(props);

//		String ln = null;
//		while ((ln = br.readLine()) != null) {
//
//			kafkaproducer.send(new ProducerRecord<String, String>(TOPIC_NAME, ln));
//			System.out.println(ln);
//		}

		Random rnd = new Random();
		br.lines().forEach(line -> {
			int partition = rnd.nextInt(5);
			kafkaproducer.send(new ProducerRecord<String, String>(TOPIC_NAME, line), new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata meta, Exception excp) {
					if (excp != null)
						excp.printStackTrace();
					else
						System.out.println(meta.offset());
				}
			});
//			System.out.println(line);
		});
		
		inputStream.close();
		fs.close();
		System.out.println("Done");

		kafkaproducer.close();
	}

}
