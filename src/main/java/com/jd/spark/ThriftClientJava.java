/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

/**
 * @Author Manh Hoang
 */
public class ThriftClientJava {
  private static final String HIVE_SERVER = "54.251.97.57";
  private static final int HIVE_SERVER_PORT = 10000;

  public static void main(String args[]) throws TTransportException {
    final TSocket tSocket = new TSocket(HIVE_SERVER, HIVE_SERVER_PORT);
    // final TProtocol protocol = new TBinaryProtocol(tSocket);
    // final HiveClient client = new HiveClient(protocol);

    tSocket.open();

    // client.execute("show tables");

    final List<String> results = /* client.fetchAll() */new ArrayList<String>();

    for (String result : results) {
      System.out.println(result);
    }
    tSocket.close();
  }
}
