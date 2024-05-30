package com.ApaceKafkaTut.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SyschronousProducer {

	public static void main(String[] args) {
		// Syschronous approach
		String topicName = "TestTopicXYZ";
		String key = "key-02";
		String value = "value-02";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092, localhost:9093");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);

		try {
			RecordMetadata metadata = producer.send(record).get();
			System.out.println(
					"Record is sent to partition no. " + metadata.partition() + "with offset : " + metadata.offset());
			System.out.println("Syschronous Producer completed with success");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Syschronous Producer failed with an exception.");

		} finally {
			producer.close();
		}

	}

}
