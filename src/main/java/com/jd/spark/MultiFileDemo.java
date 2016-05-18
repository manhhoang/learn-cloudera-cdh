/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @Author HuongHV
 */
public class MultiFileDemo {
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: Multi file demo <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("MultiFileDemo");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    JavaRDD<byte[]> binArrRDD = ctx.binaryRecords(args[0], 16);
    JavaPairRDD<Long, Double> splitRDD =
        binArrRDD.mapToPair(new PairFunction<byte[], Long, Double>() {

          private static final long serialVersionUID = 2964496670009556743L;

          public Tuple2<Long, Double> call(byte[] arg0) throws Exception {
            Long stamp = ByteUtils.bytesToLong(splitByteArray(arg0, 0));
            Double value = ByteUtils.bytesToDouble(splitByteArray(arg0, 8));
            return new Tuple2<Long, Double>(stamp, value);
          }

        });

    List<Tuple2<Long, Double>> output = splitRDD.collect();

    for (Tuple2<?, ?> tuple : output) {
      System.out.println("Time " + tuple._1() + " value " + tuple._2());
    }

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
