package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;

public class Key {
  private byte[] buffer = null;
  private int offset = 0;
//  private int keyOffset = 0;
//  private int keyLen = 0;
//  private int rowOffset = 0;
//  private int rowLen = 0;
//  private int famOffset = 0;
//  private int famLen = 0;
//  private int colOffset = 0; 
//  private int colLen = 0;
//  private int tsOffset = 0;
//  private int typeOffset = 0;
  
/*******************************************************************************
 * Constructors
 ******************************************************************************/
  public Key(Key k){
    this.buffer = k.getBuffer();
    this.offset = k.getOffset();
  }  
  public Key(KeyValue kv){
    this.buffer = kv.getBuffer();
    this.offset = kv.getOffset();
  }  
  public Key(byte[] bytes, int offset){
    this.buffer = bytes;
    this.offset = offset;
  }
  
/*******************************************************************************
 * Methods 
 ******************************************************************************/
  public byte [] getBuffer(){
    return this.buffer;
  }
  
  public int getOffset(){
    return this.offset;
  }

  public void set(Key k){
    this.buffer = k.getBuffer();
    this.offset = k.getOffset();
  }
//  public void set(KeyValue kv){
//    this.buffer = kv.getBuffer();
//    this.offset = kv.getOffset();
//  }

  
  /**
   * @return a string representing the object
   */
  @Override
  public String toString(){
    if(buffer == null){
      return "";
    }
    
    int off = 0;
    //Getting keyLen
    int keyLen = Bytes.toInt(this.buffer, off);
    off += Bytes.SIZEOF_INT;
    
    //Skipping Value length
    off += Bytes.SIZEOF_INT;
    
    //Getting row
    short rowLen = Bytes.toShort(this.buffer, off);
    off += Bytes.SIZEOF_SHORT;
    String row = rowLen <= 0 ? "" : Bytes.toString(buffer, off, rowLen);
    off += rowLen;
    
    //Getting family
    byte famLen = this.buffer[off];
    off += Bytes.SIZEOF_BYTE;
    String fam = famLen <= 0 ? "" : Bytes.toString(buffer, off, famLen);
    off += famLen;
    
    //Getting column
    int colLen = 2*Bytes.SIZEOF_INT + keyLen - off - Bytes.SIZEOF_LONG -
      Bytes.SIZEOF_BYTE;
    
    String col = colLen <= 0 ? "" : Bytes.toString(buffer, off, colLen);
    off += colLen;
    
    //Getting timestamp
    long ts = 0L;
    byte type = 0;
    if(2*Bytes.SIZEOF_INT + keyLen - off == 9){
      ts = Bytes.toLong(buffer, off, Bytes.SIZEOF_LONG);
      off += Bytes.SIZEOF_LONG;
      type = buffer[off];
    }
    
    return (row + "/" + fam + "/" + col + "/" + ts + "/" +
      KeyValue.Type.codeToType(type));
  }
  
}
