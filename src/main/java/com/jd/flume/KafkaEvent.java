package com.jd.flume;

import java.io.Serializable;

public class KafkaEvent implements Serializable {
  
  private static final long serialVersionUID = 1L;
  private long timestamp;
  private String nodeName;
  private String measureName;
  private double value;
  private String unit;
  private int groupId;
  
  /**
   * @return the timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }
  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  /**
   * @return the nodeName
   */
  public String getNodeName() {
    return nodeName;
  }
  /**
   * @param nodeName the nodeName to set
   */
  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }
  /**
   * @return the measureName
   */
  public String getMeasureName() {
    return measureName;
  }
  /**
   * @param measureName the measureName to set
   */
  public void setMeasureName(String measureName) {
    this.measureName = measureName;
  }
  /**
   * @return the value
   */
  public double getValue() {
    return value;
  }
  /**
   * @param value the value to set
   */
  public void setValue(double value) {
    this.value = value;
  }
  /**
   * @return the groupId
   */
  public int getGroupId() {
    return groupId;
  }
  /**
   * @param groupId the groupId to set
   */
  public void setGroupId(int groupId) {
    this.groupId = groupId;
  }
  /**
   * @return the unit
   */
  public String getUnit() {
    return unit;
  }
  /**
   * @param unit the unit to set
   */
  public void setUnit(String unit) {
    this.unit = unit;
  }
  
}
