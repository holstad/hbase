package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.HConstants;

public class Column {
  private byte [] column;
  private long ts;
  
//  public Column(byte [] column, int maxNrVersions) {
//    //TODO might have to put null here instead
//    this(column, HConstants.LATEST_TIMESTAMP, maxNrVersions);
//  }
//
//  public Column(byte [] column, long timestamp, int maxNrVersions) {
//    this.column = column;
//    this.ts = timestamp;
//    versions = maxNrVersions; 
//  }
  
  public Column(byte [] column, long timestamp) {
    this.column = column;
    this.ts = timestamp;
  }
  
  /**
   * 
   * @return column
   */
  public byte [] getColumn() {
    return column;
  }
  /**
   * 
   * @return timestamp
   */
  public long getTimestamp() {
    return ts;
  }
  
}
