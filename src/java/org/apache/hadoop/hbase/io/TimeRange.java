package org.apache.hadoop.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class for getting data in a specific timerange
 * The range is evaluated according to tsMax < ts <=tsMin
 *
 */
public class TimeRange {
//  private long tsMin;
//  private long tsMax;
  private byte [] tsMin = null;
  private byte [] tsMax = null;
 
  /**
   * 
   * @param tsMin the lower limit
   */
  public TimeRange(long tsMin)
  throws IOException {
    this(Long.MAX_VALUE, tsMin);
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

  public TimeRange(byte [] tsMin)
  throws IOException {
    this(Bytes.toBytes(Long.MAX_VALUE), tsMin);
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
//  /**
//   * 
//   * @return tsMin
//   */
//  public long getMinLong() {
//    return Bytes.toLong(tsMin);
//  }

  public byte [] getMax() {
    return tsMax;
  }  
//  /**
//   * 
//   * @return tsMax
//   */
//  public long getMaxLong() {
//    return Bytes.toLong(tsMax);
//  }
  
//  /**
//   * 
//   * @param ts
//   * @return 1 of ts is within range, 0 otherwise
//   */
//  public int withinRange(long ts){
//    return (tsMin <= ts && ts <= tsMax) ? 1 : 0; 
//  }
  
  // return -1 if ts > tsMax, 0 if true and 1 if ts < tsMin
  
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

}
