package com.jd.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  public static void main(String[] args) throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    Connection con =
        DriverManager.getConnection("jdbc:hive2://ec2-54-251-55-194.ap-southeast-1.compute.amazonaws.com:10000/emstest", "root", "cloudera");
    Statement stmt = con.createStatement();
    String tableName = "note";
    String sql = "select * from " + tableName;
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
    }

  }
}
