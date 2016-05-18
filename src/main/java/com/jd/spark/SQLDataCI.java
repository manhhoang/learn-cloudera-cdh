/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class SQLDataCI {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: SparkSQL Demo");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("ABSQLDataCIDemo");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    JavaRDD<byte[]> binArrRDD = ctx.binaryRecords(args[0], 16);

    JavaRDD<DataCI> dataCIRDD = binArrRDD.map(new Function<byte[], DataCI>() {

      private static final long serialVersionUID = 1L;

      public DataCI call(byte[] arg0) throws Exception {
        Long stamp = ByteUtils.bytesToLong(splitByteArray(arg0, 0));
        Double value = ByteUtils.bytesToDouble(splitByteArray(arg0, 8));
        DataCI dataCI = new DataCI(stamp, value);
        return dataCI;
      }
    });

    HiveContext hiveContext = new HiveContext(ctx.sc());

    DataFrame hiveSchemaDF = hiveContext.createDataFrame(dataCIRDD, DataCI.class);

    /*
     * String templateTable = "MYCIDATA"; hiveContext.registerDataFrameAsTable(hiveSchemaDF,
     * templateTable);
     * 
     * String tableName = "my_second_ci_table";
     * 
     * String createTable = "CREATE TABLE IF NOT EXISTS " + tableName +
     * "(time BIGINT, value DOUBLE) STORED AS TEXTFILE"; hiveContext.sql(createTable);
     * 
     * String insertTable = "INSERT OVERWRITE TABLE  " + tableName +
     * " SELECT time as time, value as value FROM " + templateTable; hiveContext.sql(insertTable);
     * 
     * List<DataCI> topCiData = hiveContext.sql("SELECT time, value FROM " + templateTable +
     * " ORDER BY time DESC") .toJavaRDD().map(new Function<Row, DataCI>() {
     * 
     * private static final long serialVersionUID = 1L;
     * 
     * public DataCI call(Row row) throws Exception { Long time = row.getLong(0); Double value =
     * row.getDouble(1); return (new DataCI(time, value)); }
     * 
     * }).collect();
     * 
     * for (DataCI data : topCiData) { System.out.println("Time " + data.getTime() + " value " +
     * data.getValue()); }
     */

    String satTable = "SAT_CI_TABLE_B";

    // hiveSchemaDF.registerTempTable(satTable);

    hiveSchemaDF.saveAsTable(satTable, SaveMode.Append);

    // hiveContext.cacheTable(satTable);

    ctx.stop();
    // ctx.close();
  }

  /**
   * 
   * @param toSplit
   * @param offset
   * @return
   */
  private static byte[] splitByteArray(byte[] toSplit, int offset) {
    byte[] split = new byte[8];
    for (int i = 0; i < split.length; i++) {
      split[i] = toSplit[offset + i];
    }

    return split;
  }
}
