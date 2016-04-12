package com.jd.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ImpalaJdbcClient {
  static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
  private static final String CONNECTION_URL = "jdbc:impala://ec2-54-251-55-194.ap-southeast-1.compute.amazonaws.com:21050/emstest";

  public static void main(String[] args) {
    Connection con = null;
    Statement stmt = null;
    ResultSet rs = null;
    String query = "select * from note";
    try {
      Class.forName(JDBC_DRIVER);
      con = DriverManager.getConnection(CONNECTION_URL);
      stmt = con.createStatement();
      rs = stmt.executeQuery(query);
      while (rs.next()) {
        System.out.println(String.valueOf(rs.getInt(1)) + "\t" + rs.getString(2));
      }
    } catch (SQLException se) {
      se.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Perform clean up
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (SQLException se1) {
      }
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException se2) {
      }
      try {
        if (con != null) {
          con.close();
        }
      } catch (SQLException se4) {
      }
    }
  }
}