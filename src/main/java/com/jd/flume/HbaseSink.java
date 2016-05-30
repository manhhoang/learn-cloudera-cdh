package com.jd.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.hbase.HbaseClient;

public class HbaseSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory.getLogger(HbaseSink.class);

  // private String myProp;

  @Override
  public void configure(Context context) {
    // String myProp = context.getString("myProp", "defaultValue");

    // Process the myProp value (e.g. validation)

    // Store myProp for later retrieval by process() method
    // this.myProp = myProp;
  }

  @Override
  public void start() {
    // Initialize the connection to the external repository (e.g. HDFS) that
    // this Sink will forward Events to ..
  }

  @Override
  public void stop() {
    // Disconnect from the external respository and do any
    // additional cleanup (e.g. releasing resources or nulling-out
    // field values) ..
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    // Start transaction
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();
    try {
      // This try clause includes whatever Channel operations you want to do

      Event event = ch.take();

      // Send the Event to the external repository.
      store(event);

      txn.commit();
      status = Status.READY;
    } catch (Throwable t) {
      txn.rollback();

      // Log exception, handle individual exceptions as needed

      status = Status.BACKOFF;

      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      txn.close();
    }
    return status;
  }

  private void store(Event event) {
    String body = new String(event.getBody());
    JSONParser jsonParser = new JSONParser();
    JSONObject jsonObject = null;
    try {
      jsonObject = (JSONObject) jsonParser.parse(body);
    } catch (ParseException e) {
      logger.info("Received this log message that is not formatted in json: " + body + "\n");
    }
    long timestamp = Long.parseLong(jsonObject.get("timestamp").toString()) / 1000 / 60;
    timestamp = timestamp * 1000 * 60;
    String nodeName = jsonObject.get("nodeName").toString();
    String measureName = jsonObject.get("measureName").toString();
    String value = jsonObject.get("value").toString();
    String unit = jsonObject.get("unit").toString();
    String groupId = jsonObject.get("groupId").toString();

    String tableName = "table_" + groupId;
    String rowKey = timestamp + "_" + nodeName + "_" + measureName;
    String[] familys = {"NodeInfo"};
    try {
      HbaseClient.creatTable(tableName, familys);
      String foundValue = HbaseClient.getOneRecord(tableName, rowKey, "value");
      float curValue = Float.parseFloat(value) + Float.parseFloat(foundValue);
      String foundCount = HbaseClient.getOneRecord(tableName, rowKey, "count");
      int curCount = Integer.parseInt(foundCount) + 1;
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "timestamp", String.valueOf(timestamp));
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "nodeName", nodeName);
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "measureName", measureName);
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "value", String.valueOf(curValue));
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "unit", unit);
      HbaseClient.addRecord(tableName, rowKey, "NodeInfo", "count", String.valueOf(curCount));
    } catch (Exception e) {
      logger.error("Fail to create Hbase table " + tableName + ":" + e.getMessage(), e);
    }
  }
}