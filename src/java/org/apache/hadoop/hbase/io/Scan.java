package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class Scan {
  private byte[] startRow = null;
  private byte[] stopRow = null;
  private Map<byte[], Set<byte[]>> familyMap =
    new TreeMap<byte[], Set<byte[]>>(Bytes.BYTES_COMPARATOR);
  
  private int maxVersions = 0;
  private RowFilterInterface filter = null;
  private TimeRange tr = new TimeRange();
  
  
//  public Scan(){}
  
  public Scan(byte[] startRow){
    this.startRow = startRow;
  }
  
  public Scan(byte[] startRow, byte[] stopRow){
    this.startRow = startRow;
    this.stopRow = stopRow;
  }
  
  /**
   * This method removes all entries for a family that were previously inserted
   * and adds the raw family to get all qualifiers for it.
   * @param family
   */
  public void addFamily(byte[] family){
    familyMap.remove(family);
    familyMap.put(family, null);
  }
  
  public void addColumn(byte[] family, byte[] qualifier){
    Set<byte[]> set = familyMap.get(family);
    if(set == null){
      set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    }
    set.add(qualifier);
    familyMap.put(family, set);
  }
  
  public void setTimeRange(long maxStamp, long minStamp)
  throws IOException {
    tr = new TimeRange(maxStamp, minStamp);
  }
  
  public void setTimeStamp(long timestamp) {
    tr = new TimeRange(timestamp);
  }
  
  public void setMaxVersions(int maxVersions){
    this.maxVersions = maxVersions;
  }
  
  public void setFilter(RowFilterInterface filter){
    this.filter = filter;
  }
  
  public void setFamilyMap(Map<byte[], Set<byte[]>> familyMap){
    this.familyMap = familyMap;
  }
  
  
  public Map<byte[], Set<byte[]>> getFamilyMap(){
    return this.familyMap;
  }
  
  public byte[] getStartRow(){
    return this.startRow;
  }

  public byte[] getStopRow(){
    return this.stopRow;
  }
  
  public int getMaxVersions(){
    return this.maxVersions;
  } 

  public TimeRange getTimeRange(){
    return this.tr;
  } 
  
  public RowFilterInterface getFilter(){
    return filter;
  }
  
}
