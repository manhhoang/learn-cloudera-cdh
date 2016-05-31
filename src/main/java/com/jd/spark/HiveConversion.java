package com.jd.spark;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class HiveConversion {

  public static void main(String[] args) throws SQLException, IOException, InterruptedException {
    SparkConf sparkConf = new SparkConf().setAppName("HiveConversion");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    HiveContext hiveContext = new HiveContext(ctx.sc());
    Row[] results = hiveContext.sql("SELECT * FROM gkadmin.node_raw").collect();
    List<Node> nodes = new ArrayList<Node>();
    for (Row row : results) {
      Node node = new Node();
      node.setId(Long.parseLong(String.valueOf(row.getAs("id"))));
      node.setName(String.valueOf(row.getAs("name")));
      node.setDisplayName(String.valueOf(row.getAs("displayname")));
      node.setResource(String.valueOf(row.getAs("type")));
      node.setGroupId(Integer.parseInt(String.valueOf(row.getAs("groupid"))));
      String genericcolumn = String.valueOf(row.getAs("genericcolumn"));
      String value = String.valueOf(row.getAs("value"));
      if (genericcolumn.equals("floor_area") && !value.trim().equals("")) {
        node.setFloor_area(Float.parseFloat(value));
      }
      if (genericcolumn.equals("occupant")) {
        node.setOccupant(Integer.parseInt(value));
      }
      if (genericcolumn.equals("owner_name")) {
        node.setOwner_name(value);
      }
      if (genericcolumn.equals("built_date")) {
        node.setBuilt_date(Long.parseLong(value));
      }
      if (genericcolumn.equals("green_mark")) {
        node.setGreen_mark(value);
      }
      if (genericcolumn.equals("node_group")) {
        node.setNode_group(value);
      }
      if (genericcolumn.equals("longitude")) {
        node.setLongitude(value);
      }
      if (genericcolumn.equals("latitude")) {
        node.setLatitude(value);
      }
      if (genericcolumn.equals("locationType")) {
        node.setLocationType(value);
      }
      nodes.add(node);
    }

    JavaRDD<Node> nodeRDD = ctx.parallelize(nodes);
    DataFrame hiveSchemaDF = hiveContext.createDataFrame(nodeRDD, Node.class);
    String satTable = "gkadmin.node";
    hiveSchemaDF.saveAsTable(satTable, SaveMode.Append);
    ctx.stop();
  }

}
