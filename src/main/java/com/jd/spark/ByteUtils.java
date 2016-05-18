/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.nio.ByteBuffer;

/**
 * @Author
 */
public class ByteUtils {

  public static byte[] longToBytes(long x) {
    return ByteBuffer.allocate(8).putLong(x).array();
  }

  public static long bytesToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes, 0, bytes.length);
    buffer.flip(); // need flip
    return buffer.getLong();
  }

  public static byte[] doubleToBytes(double x) {
    return ByteBuffer.allocate(8).putDouble(x).array();
  }

  public static double bytesToDouble(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes, 0, bytes.length);
    buffer.flip(); // need flip
    return buffer.getDouble();
  }
}
