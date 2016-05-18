/**
 * Project owner and Copyright permission
 */
package com.jd.spark;

import java.io.Serializable;

/**
 * The DataCI class 
 * 
 * @Author HuongHV
 */
public class DataCI implements Serializable {

  private static final long serialVersionUID = 1955515887338447729L;

  private long time;
  private double value;
  

  public DataCI() {
    super();
  }

  public DataCI(long time, double value) {
    super();
    this.time = time;
    this.value = value;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }
}
