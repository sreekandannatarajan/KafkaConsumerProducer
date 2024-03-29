package com.ust.producer.KafkaProdcer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer2 {
	
	public static void main(String[] args) {
		consumer2();
		
	    }  
	
public static void consumer2(){
		
		System.out.println("The Consumer 2");
		String topic = "TWITTER";//args[0].toString();
	      String group = "test";
	      Properties props = new Properties();
	      props.put("bootstrap.servers", "localhost:9093");
	      props.put("group.id", group);
	      props.put("enable.auto.commit", "true");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer",          
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	      
	      consumer.subscribe(Arrays.asList(topic));
	      System.out.println("Subscribed to topic in consumer 2" + topic);
	      int i = 0;
	         
	      while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	            for (ConsumerRecord<String, String> record : records)
	               System.out.printf("offset = %d, key = %s, value = %s\n", 
	               record.offset(), record.key(), record.value());
	      }     

	}

}
