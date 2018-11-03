package com.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProduerExample {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		try 
		{
			for(int i=1101; i<1501; i++) {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>("my_topic", Integer.toString(i),"My new message "+i);
				producer.send(record);
			}
		}
		catch (Exception e) 
		{
			// TODO: handle exception
		}finally {
			producer.close();
		}
		
	}
}
