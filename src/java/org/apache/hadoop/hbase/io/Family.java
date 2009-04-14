package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;


/**
 * The number of versions to fetch are kept in the Get class but the current
 * count is kept as the last byte in every column, and is set to 0 when adding
 * the columns. 
 *
 */
public class Family implements Writable{
  private byte [] family = new byte [0];
  private byte [][] columns = new byte [0][];
  
  public Family(){}
  
  public Family(byte [] family){
    this.family = family;
  }
  /**
   * 
   * @param family
   * @param column
   */
  public Family(byte [] family, byte [] column) {
    this(family, new byte[][] {column});
  }

  public Family(byte [] family, byte [][] columns) {
    this.family = family;
    this.columns = new byte [columns.length][];
    this.columns = addVersionsToTheEndOfColumns(columns);
  }
  
  
  /**
   * 
   * @return the family
   */
  public byte [] getFamily() {
    return family;
  }
  /**
   * Sets the family name of this Family
   * @param family
   */
  public void setFamily(byte [] family){
    this.family = family;
  }
  
  public byte [][] getColumns() {
    return columns;
  }
 
  public void setColumns(byte [][] columns) {
    this.columns = new byte [columns.length][];
    this.columns = addVersionsToTheEndOfColumns(columns);
  }
  
  /**
   * 
   * @return boolean
   */
  public boolean isEmpty(){
    return columns == null || columns.length == 0;
  }

  
  private byte [][] addVersionsToTheEndOfColumns(byte [][] columns){
    int len = columns.length;
    byte [][] fixedColumns = new byte [len][];
    for(int i=0; i<len; i++){
      fixedColumns[i] = appendVersionsFetched(columns[i]);
    }
    return fixedColumns;
  }
  
  private byte [] appendVersionsFetched(byte [] column){
    return appendByte(column, (byte)0);
  }
  
  private byte[] appendByte(byte [] column, byte b){
   int len = column.length;
   byte [] ret = new byte [len + 1];
   System.arraycopy(column, 0, ret, 0, len);
   ret[len] = b;
   return ret;
  }
  
  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();

    sb.append("Family ");
    sb.append(new String(family));
    sb.append(", columns [");
    int i = 0;
    for(; i<columns.length-1; i++){
      sb.append(new String(columns[i], 0, columns[i].length -1));
      sb.append(", ");      
    }

    sb.append(new String(columns[i], 0, columns[i].length -1));
    sb.append("]"); 
    return sb.toString();
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.family = Bytes.readByteArray(in);
    int nColumns = in.readInt();
    this.columns = new byte [nColumns][];
    for(int i=0; i<nColumns; i++){
      columns[i] = Bytes.readByteArray(in);
    }
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.family);
    out.writeInt(columns.length);
    for(byte [] column : columns){
      Bytes.writeByteArray(out, column);
    }
  }
  
}
