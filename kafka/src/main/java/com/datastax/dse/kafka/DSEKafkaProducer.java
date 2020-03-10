package com.datastax.dse.kafka;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonObject;

public class DSEKafkaProducer {

	public static void main(String[] args) {

		// Assign topicName to string variable
		String topicName = "dsetopic";

		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", 16384);

		// Reduce the no of requests less than 0
		props.put("linger.ms", 1);

		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer;// = new KafkaProducer<String, String>(props);

		// Date and Time Formatter
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		LocalDateTime now = LocalDateTime.now();

		// Create new JSON Object
		JsonObject person;
		for (int i = 1; i <= 100; i++) {
			 producer = new KafkaProducer<String, String>(props);
			person = new JsonObject();
			person.addProperty("firstname", "Sergey" + i);
			person.addProperty("lastname", "Kargopolov");
			person.addProperty("birthdate", dtf.format(now));

			producer.send(new ProducerRecord<String, String>(topicName, "DSE-Message" + i, person.toString()));
			producer.close();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(person.toString());
		}

		System.out.println("Message sent successfully");
	}

}
