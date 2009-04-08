package org.apache.hadoop.hbase.io;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * The number of versions to fetch are kept in the Get class but the current
 * count is kept as the last byte in every column, and is set to 0 when adding
 * a new column. 
 *
 */
public class Family {
  private byte [] family = null;
  private SortedSet<byte[]> cols = null;
  private List<byte[]> columns = null;
  private boolean modified = false;
  
  public Family(){}
  
  public Family(byte [] family){
    this.family = family;
  }
//  public Family(byte [] family, int versions){
//    this(family, null, versions);
//  } 
  /**
   * 
   * @param family
   * @param column
   */
  public Family(byte [] family, byte [] column) {
    this.family = family;
    this.cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    this.cols.add(appendByte(column, (byte)0));
  }
//  public Family(byte [] family, byte [] column, int versions) {
//    this.family = family;
//    this.cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
//    this.versions = versions;
//    this.cols.add(Bytes.add(column, Bytes.toBytes(versions)));
//  }
  
  /**
   * 
   * @param column
   */
  public void add(byte [] column) {
    this.cols.add(appendByte(column, (byte)0));
    modified = true;
  }

  /**
   * 
   * @param column
   * @return true if column has been removed false otherwise
   */
  public boolean remove(byte [] column){
    boolean ret = cols.remove(appendByte(column, (byte)0));
    if (ret){
      modified = true;
    }
    return ret;
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
  
  /**
   * 
   * @return the array of columns in this family
   */
  public List<byte[]> getColumns() {
    if(columns == null || modified){
      //TODO this should be done when data is passed from the client and not at
      //the server
      columns = new LinkedList<byte[]>(cols);
      modified = false;
    }
    return columns;
  }
  
  /**
   * 
   * @return boolean
   */
  public boolean isEmpty(){
    return columns.isEmpty();
  }

  /**
   * 
   * @return boolean
   */
  public void clear(){
    columns.clear();
  }
  
  private byte[] appendByte(byte [] column, byte b){
   int len = column.length;
   byte [] ret = new byte [len + 1];
   System.arraycopy(column, 0, ret, 0, len);
   ret[len] = 0;
   return ret;
  }
  
}
