package com.jd.spark;

import java.io.Serializable;

public class Node implements Serializable {
  private static final long serialVersionUID = 1L;

  private long id;
  private String name;

  public Node(long id, String name) {
    this.id = id;
    this.name = name;
  }

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
}
