package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import java.util.List;

import org.apache.hadoop.hbase.filter.RowFilterInterface;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.TimeRange;

public interface ServerGet {
  /**
   * Sets all the internal variables to original settings, needs to be done
   * in between every storeFile
   */
  public void clear();
  
  /**
   * 
   * Return codes that need to be implemented by method
   * -1 if cur should not be included in result
   * 0 if cur should be included in result
   * 1 if cur has reached the put/delete border
   * 2 if cur has reached the next row
   * 3 if cur is past this StoreFiles boundary for this Get
   * 4 if cur is past this Stores boundary for this get
   * 
   * @param kv
   * @return the return code 
   */
  public int compareTo(KeyValue kv) throws IOException;
  /**
   * 
   * Return codes that need to be implemented by method
   * -1 if cur should not be included in result
   * 0 if cur should be included in result
   * 1 if cur has reached the put/delete border
   * 2 if cur has reached the next row
   * 3 if cur is past this StoreFiles boundary for this Get
   * 4 if cur is past this Stores boundary for this get
   * 
   * @param kv
   * @param multiFamily, if the store includes multiple families
   * @return the return code 
   */
  public int compareTo(KeyValue kv, boolean multiFamily) throws IOException;
  
//  public byte [] getFirstColumn();
  public byte [] getRow();
  
  public TimeRange getTimeRange();
  
  public List<byte[]> getColumns();
//  public void setColumns(List<byte[]> columns);
//  public byte [][] getColumns();
  public void setColumns(byte [][] columns);
  
  
  public void setDeletes(List<Key> deletes);

  public byte [] getFamily();
  public void setFamily(byte [] family);
  
  public void setFilter(RowFilterInterface filter);
  
  public long getNow();
  public void setNow();
  
  public long getTTL();
  public void setTTL(long ttl);
  
  public Deletes mergeDeletes(List<Key> l1, List<Key> l2);
  public Deletes mergeDeletes(List<Key> l1, List<Key> l2, boolean multiFamily);
  
  public String toString();
  
}
