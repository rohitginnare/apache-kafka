package com.ApaceKafkaTut.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

	public static void main(String[] args) {
		//fire and forget approach
		String topicName = "TestTopicXYZ" ;
		String key = "key-1";
		String value = "value-1";
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		
		producer.send(record);
		producer.close();
		
		System.out.println("Completed Simple Producer");
	}

}
