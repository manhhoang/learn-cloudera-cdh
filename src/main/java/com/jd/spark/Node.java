package com.jd.spark;

import java.io.Serializable;

public class Node implements Serializable {
  private static final long serialVersionUID = 1L;

  private long id;
  private String name;
  private String displayName;
  private String resource;
  private String loadType;
  private int groupId;
  private float floor_area;
  private int occupant;
  private String owner_name;
  private String longitude;
  private String latitude;
  private String node_group;
  private String widget;
  private long built_date;
  private String green_mark;
  private String locationType;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getLoadType() {
    return loadType;
  }

  public void setLoadType(String loadType) {
    this.loadType = loadType;
  }

  public int getGroupId() {
    return groupId;
  }

  public void setGroupId(int groupId) {
    this.groupId = groupId;
  }

  public float getFloor_area() {
    return floor_area;
  }

  public void setFloor_area(float floor_area) {
    this.floor_area = floor_area;
  }

  public int getOccupant() {
    return occupant;
  }

  public void setOccupant(int occupant) {
    this.occupant = occupant;
  }

  public String getOwner_name() {
    return owner_name;
  }

  public void setOwner_name(String owner_name) {
    this.owner_name = owner_name;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

  public String getNode_group() {
    return node_group;
  }

  public void setNode_group(String node_group) {
    this.node_group = node_group;
  }

  public String getWidget() {
    return widget;
  }

  public void setWidget(String widget) {
    this.widget = widget;
  }

  public long getBuilt_date() {
    return built_date;
  }

  public void setBuilt_date(long built_date) {
    this.built_date = built_date;
  }

  public String getGreen_mark() {
    return green_mark;
  }

  public void setGreen_mark(String green_mark) {
    this.green_mark = green_mark;
  }

  public String getLocationType() {
    return locationType;
  }

  public void setLocationType(String locationType) {
    this.locationType = locationType;
  }
}
