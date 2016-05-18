/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author Manh Hoang
 * @Version
 * @Date
 */
public class HiveJdbcClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";

  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }

    // replace "hive" here with the name of the user the queries should run as

    Connection con = DriverManager.getConnection("jdbc:hive2://54.251.97.57:10000", "", "");

    Statement stmt = con.createStatement();

    // show tables
    String sql1 = "show tables";
    System.out.println("Running: " + sql1);
    ResultSet res1 = stmt.executeQuery(sql1);
    if (res1.next()) {
      System.out.println(res1.getString(1));
    }

    String tableName = "dataci_table";

    // select * query
    String sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(String.valueOf(res.getLong(1)) + "\t" + res.getDouble(2));
    }

  }
}
