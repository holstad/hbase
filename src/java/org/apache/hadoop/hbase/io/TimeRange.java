package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Class for getting data in a specific timerange
 * The range is evaluated according to tsMax < ts <=tsMin
 *
 */
public class TimeRange implements Writable{
  private byte [] tsMin = new byte [0];
  private byte [] tsMax = new byte [0];

  /**
   * Default constructor
   */
  public TimeRange(){
    this(0);
  } 
  /**
   * 
   * @param tsMin the lower limit
   */
  public TimeRange(long tsMin){
    this(Bytes.toBytes(tsMin));
  }
  public TimeRange(byte [] tsMin) {
    this.tsMax = Bytes.toBytes(Long.MAX_VALUE);
    this.tsMin = tsMin;
  }
  
  /**
   * 
   * @param tsMax the upper limit 
   * @param tsMin the lower limit
   */
  public TimeRange(long tsMax, long tsMin)
  throws IOException {
    this(Bytes.toBytes(tsMax), Bytes.toBytes(tsMin));
  }

  public TimeRange(byte [] tsMax, byte [] tsMin)
  throws IOException {
    int ret = Bytes.compareTo(tsMax, tsMin);
    if(ret <= -1){
      throw new IOException("tsMax is smallar than tsMin");
    }
    this.tsMax = tsMax;
    this.tsMin = tsMin;
  }

  
  public byte [] getMin() {
    return tsMin;
  }

  public byte [] getMax() {
    return tsMax;
  }  
  
  
  /**
   * This method checks is tsMax < ts <=tsMin
   * 
   * @param bytes the timestamp to compare to
   * @param offset the offset into the bytes where to start comparing
   * 
   * @return returns 0 if not within and 1 if within tsMax > ts >= tsMin
   */
  public int withinTimeRange(byte [] bytes, int offset){
    int ret = Bytes.compareTo(tsMin, 0, Bytes.SIZEOF_LONG, bytes,
      offset, Bytes.SIZEOF_LONG);
    if(ret == 0){
      return 1;
    } else if(ret >= 1){
      return 0;
    }
    
    //check if ts < tsMax
    ret = Bytes.compareTo(tsMax, 0, Bytes.SIZEOF_LONG, bytes,
      offset, Bytes.SIZEOF_LONG);
    if(ret <= 0){
      return 0;
    }
    return 1;
  }

  public int withinTimeRange(long ts){
//    int ret = Bytes.compareTo(tsMin, 0, Bytes.SIZEOF_LONG, bytes,
//      offset, Bytes.SIZEOF_LONG);
    long tsmin = Bytes.toLong(tsMin);
    long ret = tsmin - ts;
    if(ret == 0){
      return 1;
    } else if(ret >= 1){
      return 0;
    }
    
    //check if ts < tsMax
    long tsmax = Bytes.toLong(tsMax);
    ret = tsmax - ts;
    if(ret <= 0){
      return 0;
    }
    return 1;
  }
  
  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();
    sb.append("tsMax ");
    sb.append(Bytes.toLong(tsMax));
    sb.append(", tsMin");
    sb.append(Bytes.toLong(tsMin));
    return sb.toString();
  }
  
//Writable
  public void readFields(final DataInput in) throws IOException {
    this.tsMin = Bytes.readByteArray(in);
    this.tsMax = Bytes.readByteArray(in);
  }
  
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.tsMin);
    Bytes.writeByteArray(out, this.tsMax);
  }
  
}
