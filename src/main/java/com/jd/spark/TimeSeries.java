package com.jd.spark;

public class TimeSeries {

  private long timestamp;
  private String nodeName;
  private String measureName;
  private String unit;
  private float value;

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getNodeName() {
    return nodeName;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }

  public String getMeasureName() {
    return measureName;
  }

  public void setMeasureName(String measureName) {
    this.measureName = measureName;
  }

  public String getUnit() {
    return unit;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  public float getValue() {
    return value;
  }

  public void setValue(float value) {
    this.value = value;
  }

}
