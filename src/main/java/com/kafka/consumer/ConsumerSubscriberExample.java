package com.kafka.consumer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class ConsumerSubscriberExample {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "test-consumer-group");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
//		ArrayList<String> topics = new ArrayList<String>();
//		topics.add("my_topic");
//		
//		consumer.subscribe(topics);
		
		ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
		
		TopicPartition partition1 = new TopicPartition("my_topic", 0);
		TopicPartition partition2 = new TopicPartition("my_topic", 1);
		TopicPartition partition3 = new TopicPartition("my_topic", 2);
		
		partitions.add(partition1);
		partitions.add(partition2);
		partitions.add(partition3);
		
		consumer.assign(partitions);
		
		try {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for(ConsumerRecord<String, String> record : records) {
					System.out.println(String.format("Topic %s, Partition %d, Offset %d, Key %s, value %s", 
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			consumer.close();
		}
	}

}
