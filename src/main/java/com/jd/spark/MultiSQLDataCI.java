/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * @Author HuongHV
 */
public class MultiSQLDataCI {

  public static void main(String[] args) {

    String fileName = "file:/E:/TVO/Dev64/workspace/SparkDemo/data/*";

    SparkConf sparkConf = new SparkConf().setAppName("MultiSQLDataCIDemo");
    sparkConf.setMaster("local[2]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    JavaPairRDD<String, PortableDataStream> multiRDD = ctx.binaryFiles(/* args[0] */fileName);

    JavaPairRDD<String, byte[]> ts = multiRDD.mapValues(new Function<PortableDataStream, byte[]>() {

      private static final long serialVersionUID = 2139242591795507470L;

      public byte[] call(PortableDataStream v1) throws Exception {
        byte[] data = v1.toArray();
        return data;
      }
    });

    JavaRDD<byte[]> nts = ts.values();

    JavaRDD<List<DataCI>> endtab = nts.map(new Function<byte[], List<DataCI>>() {

      private static final long serialVersionUID = 1L;

      public List<DataCI> call(byte[] data) throws Exception {

        List<DataCI> rdata = new ArrayList<DataCI>();
        for (int i = 0, j = 0; j < data.length; ++i, j = i * 16) {
          byte[] oneFrame = Arrays.copyOfRange(data, j, j + 16);
          Long stamp = ByteUtils.bytesToLong(splitByteArray(oneFrame, 0));
          Double value = ByteUtils.bytesToDouble(splitByteArray(oneFrame, 8));
          DataCI dataCI = new DataCI(stamp, value);
          rdata.add(dataCI);
        }

        return rdata;
      }

    });

    List<List<DataCI>> output = endtab.collect();

    List<DataCI> total = new ArrayList<DataCI>();

    for (List<DataCI> out : output) {
      for (DataCI o : out) {
        System.out.println("Time " + o.getTime() + " value " + o.getValue());
        total.add(o);
      }
    }

    JavaRDD<DataCI> fdata = ctx.parallelize(total);

    List<DataCI> elstdata = fdata.collect();

    System.out.println("New result data " + elstdata.size());

    for (DataCI e : elstdata) {
      System.out.println("Time " + e.getTime() + " value " + e.getValue());
    }

    HiveContext hiveContext = new HiveContext(ctx.sc());

    DataFrame hiveSchemaDF = hiveContext.createDataFrame(fdata, DataCI.class);

    hiveSchemaDF.saveAsTable("DATA_CI_TABLE"/* , SaveMode.Append */);

    ctx.stop();
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
