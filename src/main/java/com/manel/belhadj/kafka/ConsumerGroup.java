package com.manel.belhadj.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerGroup implements Runnable {

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;

	public ConsumerGroup(int id, String groupId, List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", "manel-pc:6667");
		props.put("group.id", groupId + "101");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.consumer = new KafkaConsumer<>(props);
	}

	public void run() {
		try {
			consumer.subscribe(topics);
			System.out.println("11111111111111111");
			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(100); //Long.MAX_VALUE
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println(this.id + ": " + data);
				}
			}
		} catch (WakeupException e) {
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
		// consumer.close();
	}

	public static void main(String[] args) {

		int numConsumers = 3;
		String groupId = "g02";
		List<String> topics = Arrays.asList("kafkaTopicThree");
		ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

		final List<ConsumerGroup> consumers = new ArrayList<>();
		for (int i = 0; i < numConsumers; i++) {
			System.out.println("consumer n°: " + i);
			ConsumerGroup consumer = new ConsumerGroup(i, groupId, topics);
			consumers.add(consumer);
			executor.submit(consumer);
		}
		//save the data before shutdown (save then shutdown)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				for (ConsumerGroup consumer : consumers) {
					consumer.shutdown();
				}
				executor.shutdown();
				try {
					executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

}
