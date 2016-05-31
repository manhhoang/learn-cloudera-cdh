package com.jd.spark;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class ImportBinDataHbase {

  public static void main(String[] args) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, -1);
    SparkConf sparkConf = new SparkConf().setAppName("BinData");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    HiveContext hiveContext = new HiveContext(jsc.sc());
    Row[] results =
        hiveContext
            .sql(
                "FROM gkadmin.emstestevents_raw SELECT timestamp,nodename,measurename,unit,value WHERE timestamp >="
                    + cal.getTimeInMillis()).collect();
    List<TimeSeries> listTime = new ArrayList<TimeSeries>();
    for (Row row : results) {
      TimeSeries time = new TimeSeries();
      time.setTimestamp(Long.parseLong((String) row.getAs("timestamp")));
      time.setNodeName((String) row.getAs("nodename"));
      time.setMeasureName((String) row.getAs("measurename"));
      time.setUnit((String) row.getAs("unit"));
      time.setValue(Float.parseFloat((String) row.getAs("value")));
      listTime.add(time);
    }
    JavaRDD<TimeSeries> timeSeriesRDD = jsc.parallelize(listTime);

    String tableName = "table";
    Configuration conf = HBaseConfiguration.create();
    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
    hbaseContext.bulkPut(timeSeriesRDD, TableName.valueOf(tableName), new PutFunction());

    // hbaseContext.bulkGet(TableName.valueOf(tableName), 2, timeSeriesRDD, new GetFunction(),
    // new ResultFunction());

    jsc.stop();
  }

  private static class PutFunction implements Function<TimeSeries, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(TimeSeries v) throws Exception {
      System.out.println("timestamp" + v.getTimestamp() + " === nodeName=" + v.getNodeName());
      Put put = new Put(Bytes.toBytes(v.getTimestamp()));
      put.addColumn(Bytes.toBytes("NodeInfo"), Bytes.toBytes("timestamp"), Bytes.toBytes(v
          .getTimestamp()));
      put.addColumn(Bytes.toBytes("NodeInfo"), Bytes.toBytes("nodeName"), Bytes.toBytes(v
          .getNodeName()));
      put.addColumn(Bytes.toBytes("NodeInfo"), Bytes.toBytes("measureName"), Bytes.toBytes(v
          .getMeasureName()));
      put.addColumn(Bytes.toBytes("NodeInfo"), Bytes.toBytes("value"), Bytes.toBytes(v.getValue()));
      return put;
    }

  }

  private static class GetFunction implements Function<TimeSeries, Get> {

    private static final long serialVersionUID = 1L;

    public Get call(TimeSeries v) throws Exception {
      return new Get(serialize(v));
    }
  }

  private static class ResultFunction implements Function<Result, String> {

    private static final long serialVersionUID = 1L;

    public String call(Result result) throws Exception {
      Iterator<Cell> it = result.listCells().iterator();
      StringBuilder b = new StringBuilder();

      b.append(Bytes.toString(result.getRow())).append(":");

      while (it.hasNext()) {
        Cell cell = it.next();
        String q = Bytes.toString(cell.getQualifierArray());
        if (q.equals("counter")) {
          b.append("(").append(Bytes.toString(cell.getQualifierArray())).append(",").append(
              Bytes.toLong(cell.getValueArray())).append(")");
        } else {
          b.append("(").append(Bytes.toString(cell.getQualifierArray())).append(",").append(
              Bytes.toString(cell.getValueArray())).append(")");
        }
      }
      return b.toString();
    }
  }

  private static byte[] serialize(Object obj) throws IOException {
    try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
      try (ObjectOutputStream o = new ObjectOutputStream(b)) {
        o.writeObject(obj);
      }
      return b.toByteArray();
    }
  }
}
