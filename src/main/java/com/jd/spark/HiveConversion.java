package com.jd.spark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class HiveConversion {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException, IOException, InterruptedException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    // System.setProperty("java.security.auth.login.config", "gss-jaas.conf");
    System.setProperty("java.security.krb5.conf", "krb5.conf");
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    System.setProperty("sun.security.krb5.debug", "false");

    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab("hive@EXAMPLE.COM", "hive.keytab");
    Connection con =
        DriverManager
            .getConnection("jdbc:hive2://ec2-54-151-149-245.ap-southeast-1.compute.amazonaws.com:10000/gkadmin;principal=hive/ip-10-167-7-239.ap-southeast-1.compute.internal@EXAMPLE.COM");
    Statement stmt = con.createStatement();
    String sql = "select * from node order by id ASC limit 100";
    ResultSet res = stmt.executeQuery(sql);
    List<Node> nodes = new ArrayList<Node>();
    while (res.next()) {
      System.out.println(String.valueOf(res.getLong(1)) + "\t" + res.getString(2) + "\t"
          + res.getString(3) + "\t" + res.getString(4) + "\t" + res.getString(5));
      nodes.add(new Node(1, "Test"));
    }

    SparkConf sparkConf = new SparkConf().setAppName("HiveConversion");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<Node> nodeRDD = ctx.parallelize(nodes);

    HiveContext hiveContext = new HiveContext(ctx.sc());
    DataFrame hiveSchemaDF = hiveContext.createDataFrame(nodeRDD, Node.class);
    String satTable = "node_convertion";
    hiveSchemaDF.saveAsTable(satTable, SaveMode.Append);
    ctx.stop();
  }

  private static byte[] splitByteArray(byte[] toSplit, int offset) {
    byte[] split = new byte[8];
    for (int i = 0; i < split.length; i++) {
      split[i] = toSplit[offset + i];
    }

    return split;
  }

}
