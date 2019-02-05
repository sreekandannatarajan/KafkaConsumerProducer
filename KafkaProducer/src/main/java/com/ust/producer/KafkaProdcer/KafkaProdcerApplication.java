package com.ust.producer.KafkaProdcer;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaProdcerApplication implements Partitioner {
	private static final int PARTITION_COUNT = 50;
	static Properties configProperties = new Properties();
	static String line = null;
	static String topicName = "POST";
	static Producer<Long, String> producer = null;
	static {
		if (configProperties.isEmpty()) {
			configProperties = new Properties();
			configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "UST-1234");
			//configProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "UST-TRASID-12789");
			configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					LongSerializer.class.getName());
			configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class.getName());
			configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					"localhost:9092,localhost:9093");
		}
	}
//transactional.id
	public static void main(String[] args) {

		for (int i = 1; i <= 100; i++) {
			String msg = " *** The message is published with id ***** " + i;
			try {
				if (i % 2 == 0) {
					publishToQueue(msg, 0);
				} else {
					publishToQueue(msg, 1);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static <K, V> void publishToQueue(final String message,
			final int partion) {
		try {
			long time = System.currentTimeMillis();
			System.out.println(" The Method is  publishToQueue ");
			ProducerRecord<Long, String> rec = new ProducerRecord<Long, String>(
					topicName, partion, time, message);
			producer = new KafkaProducer<Long, String>(configProperties);
			//producer.beginTransaction();
			RecordMetadata id = producer.send(rec).get();
			System.out.println(" The value is offset " + id.offset());
			System.out.println(" The value is partition " + id.partition());
			// producer.commitTransaction();
		} catch (Exception ex) {
			System.out.println("The exception is " + ex);
			 producer.abortTransaction();
			 producer.flush();
		} finally {
			producer.flush();
			producer.close();
		}

		/*
		 * producer.send(rec, new Callback() { public void
		 * onCompletion(RecordMetadata metadata, Exception exception) {
		 * System.out.println("Message sent to topic ->" + metadata.topic()
		 * +" stored at offset->" + metadata.offset()); } });
		 */
	}

	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	public int partition(String topic, Object key, byte[] keyBytes,
			Object value, byte[] valueBytes, Cluster cluster) {
		Integer keyInt = Integer.parseInt(key.toString());
		return keyInt % PARTITION_COUNT;

	}

	public void close() {
		// TODO Auto-generated method stub

	}

}
