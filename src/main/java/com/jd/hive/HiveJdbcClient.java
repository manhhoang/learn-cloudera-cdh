package com.jd.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException, IOException, InterruptedException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		System.setProperty("java.security.auth.login.config", "gss-jaas.conf");
		System.setProperty("java.security.krb5.conf", "krb5.conf");
		System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
		System.setProperty("sun.security.krb5.debug", "true");

		// org.apache.hadoop.conf.Configuration conf = new
		// org.apache.hadoop.conf.Configuration();
		// conf.set("hadoop.security.authentication", "kerberos");
		// UserGroupInformation.setConfiguration(conf);
		// UserGroupInformation.loginUserFromKeytab("hive@EXAMPLE.COM",
		// "hive.keytab");
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://ec2-54-151-149-245.ap-southeast-1.compute.amazonaws.com:10000/gkadmin;principal=hive/ip-10-167-7-239.ap-southeast-1.compute.internal@EXAMPLE.COM");
		Statement stmt = con.createStatement();
		String sql = "select * from EmsTestEvents order by 'timestamp' ASC limit 100";
		// String sql = "SHOW TABLES";
		ResultSet res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2) + "\t" + res.getString(3) + "\t"
					+ res.getString(4) + "\t" + res.getString(5));
			// System.out.println(String.valueOf(res.getString(1)));
		}

	}
}
