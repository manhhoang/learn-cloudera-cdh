package com.jd.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducerTest extends Thread {
  final static String TOPIC = "kafkatest";
  kafka.javaapi.producer.Producer<String, String> producer;
  private int threadLoopCount = 10;
  ProducerConfig producerConfig;

  public class Note {
    private int noteId;
    private String noteName;

    public Note() {
      super();
    }

    public Note(int id, String noteName) {
      this.noteId = id;
      this.noteName = noteName;
    }

    public int getNoteId() {
      return noteId;
    }

    public void setNoteId(int noteId) {
      this.noteId = noteId;
    }

    public String getNoteName() {
      return noteName;
    }

    public void setNoteName(String noteName) {
      this.noteName = noteName;
    }

    public String toJON() {
      return "{noteId:1,noteName=test}";
    }
  }

  public int getThreadLoopCount() {
    return threadLoopCount;
  }

  public void setThreadLoopCount(int threadLoopCount) {
    this.threadLoopCount = threadLoopCount;
  }

  public KafkaProducerTest() {
    init();
  }

  private void init() {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", "54.169.107.50:9092");
    properties.put("serializer.class", "kafka.serializer.StringEncoder");
    producerConfig = new ProducerConfig(properties);
    producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
  }

  @Override
  public void run() {
    try {
      int currentLoopCount = 0;
      while (currentLoopCount < threadLoopCount) {
        KeyedMessage<String, String> message =
            new KeyedMessage<String, String>(TOPIC, (new Note(currentLoopCount, "note"
                + currentLoopCount)).toJON());
        System.out.println("Posting Message to Kafka Server : " + message);
        producer.send(message);
        currentLoopCount++;
        Thread.sleep(10000);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Error in thread KafkaProducerTest " + e.getMessage());
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  public static void main(String[] argv) {
    KafkaProducerTest producer = new KafkaProducerTest();
    producer.setThreadLoopCount(15);
    producer.start();
  }
}
