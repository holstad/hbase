package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Writable;

public class Result implements Writable{
  private byte[] row = null;
  private KeyValue[] kvs = null;
  

  /**
   * Constructor used for Writable 
   */
  public Result(){}
  
  public Result(KeyValue[] kvs){
    this.row = kvs[0].getRow();
    this.kvs = kvs;
  }
  
  public byte[] getRow(){
    return this.row;
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
  
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.row = Bytes.readByteArray(in);
    int length = in.readInt();
    this.kvs = new KeyValue[length];
    KeyValue kv = null;
    for(int i=0; i<length; i++){
      kv = new KeyValue();
      kv.readFields(in);
      kvs[i] = kv;
    }
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.row);
    int len = kvs.length;
    out.writeInt(len);
    for(int i=0; i<len; i++){
      kvs[i].write(out);
    }
  }
  
}
