package org.apache.hadoop.hbase.client;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.KeyValue;

public class Result {
  private KeyValue[] kvs = null;
  
  public Result(KeyValue[] kvs){
    this.kvs = kvs;
  }
  
  public KeyValue[] raw(){
    return kvs;
  }
  
  /**
   * This method sorts the returned KeyValue[] in place, so after using it
   * the under laying KeyValue[] is changed
   * @return a sorted kvs
   */
  public KeyValue[] sorted(){
    Arrays.sort(kvs, (Comparator)KeyValue.KEY_COMPARATOR);
    return kvs;
  }
  
  public RowResult rowResult(){
    return RowResult.createRowResult(kvs);
  }
  
  /**
   * Method that returns the value of the first KeyValue in the underlaying 
   * KeyValue[]
   * @return value from first KeyValue byte[] 
   */
  public byte[] value(){
    return kvs[0].getValue();
  }
}
