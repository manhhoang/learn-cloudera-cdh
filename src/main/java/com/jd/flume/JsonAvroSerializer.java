package com.jd.flume;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

public class JsonAvroSerializer extends AbstractAvroEventSerializer<KafkaEvent>{

  private static final Logger logger =
      LoggerFactory.getLogger(JsonAvroSerializer.class);

  private OutputStream out;
  

  private JsonAvroSerializer() {
    super();
  }

  private JsonAvroSerializer(OutputStream out) {
    this.out = out;
  }
  
  @Override
  protected KafkaEvent convert(Event event) {
    ObjectMapper mapper = new ObjectMapper();
    KafkaEvent emsEvent = null;
    String data = new String(event.getBody(), Charsets.UTF_8).trim();
//    logger.info("Inside convert data : " + data);
    try {
      emsEvent = mapper.readValue(data, KafkaEvent.class);
    } catch (JsonParseException e) {
      logger.error("error in JsonAvroSerializer convert function " + e.getMessage(),e);
    } catch (JsonMappingException e) {
      logger.error("error in JsonAvroSerializer convert function " + e.getMessage(),e);
    } catch (IOException e) {
      logger.error("error in JsonAvroSerializer convert function " + e.getMessage(),e);
    }
//    logger.info("Inside convert method note Id : " + note.getNoteId() + " ---------  Name : " + note.getNoteName());
    return emsEvent;
  }

  @Override
  protected OutputStream getOutputStream() {
    Preconditions.checkNotNull(out);
    return out;
  }

  @Override
  protected Schema getSchema() {
    Schema avroSchema = null;
    try {
      ObjectMapper mapper = new ObjectMapper(new AvroFactory());
      AvroSchemaGenerator gen = new AvroSchemaGenerator();
      mapper.acceptJsonFormatVisitor(KafkaEvent.class, gen);
      AvroSchema schemaWrapper = gen.getGeneratedSchema();
      avroSchema = schemaWrapper.getAvroSchema();
    } catch (Exception e) {
      System.out.println("error in getSchema " + e.getMessage());
    }
//    logger.info("avro schema : " + avroSchema.toString());
    return avroSchema;
  }
  
  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      JsonAvroSerializer ser = new JsonAvroSerializer(out);
      ser.configure(context);
      return ser;
    }
  }
  
  public static void main(String[] args) {
    JsonAvroSerializer serializer = new JsonAvroSerializer();
    System.out.println(serializer.getSchema().toString());
  }
}
