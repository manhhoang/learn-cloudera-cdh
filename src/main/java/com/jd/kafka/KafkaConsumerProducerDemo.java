package com.jd.kafka;

public class KafkaConsumerProducerDemo {
	public static void main(String[] args) {
		// boolean isAsync = args.length == 0 ||
		// !args[0].trim().toLowerCase().equals("sync");
		boolean isAsync = false;
		Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
		producerThread.start();

		Consumer consumerThread = new Consumer(KafkaProperties.TOPIC);
		consumerThread.start();

	}
}
