package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Class used for putting data into HBase 
 *
 */
public class Delete implements HeapSize, Writable{
//  protected final KeyValue.Type TYPE = KeyValue.Type.Put;
  private byte[] row = null;
  private long ts = HConstants.LATEST_TIMESTAMP;
  private long rl = -1l;
  private Map<byte[], List<KeyValue>> familyMap = null;
  
  /**
   * Don't use this constructor, only for use with serialization
   */
  public Delete(){}
  
  public Delete(byte[] row){
    this.row = row;
  }
  public Delete(byte[] row, long ts, long rl){
    this.row = row;
    this.ts = ts;
    this.rl = rl;
  }
  
  public void deleteFamily(byte[] family){}
  public void deleteFamily(byte[] family, long ts){}
  
  public void deleteColumn(byte[] family, byte[] qualifier){}
  public void deleteColumn(byte[] family, byte[] qualifier, long ts){}

  public void deleteColumns(byte[] family, byte[] qualifier){}
  public void deleteColumns(byte[] family, byte[] qualifier, long ts){}
  
  public Map<byte[], List<KeyValue>> getFamilyMap(){
    return this.familyMap;
  }
  
  public byte[] getRow(){
    return this.row;
  }
  
  public long getRowLock(){
    return this.rl;
  } 
  
  
  
  public String toString(){
    StringBuffer sb = new StringBuffer();
    sb.append("Row ");
    sb.append(new String(this.row));
    sb.append(", Families[ ");
    for(Map.Entry<byte[], List<KeyValue>> entry : this.familyMap.entrySet()){
      sb.append(new String(entry.getKey()));
      sb.append(", (");
      for(KeyValue kv : entry.getValue()){
        sb.append(kv.toString());
        sb.append(", ");
      }
      sb.append("), ");
    }
    sb.append("]");    
    
    return sb.toString();
  }
  
  
  //Heapsize
  //TODO fix heapSize
  public long heapSize(){
    //Should be 16 + 24+row.length/8 + familyMap.heapSize + 8.
    //familyMap.heapSize 
    return 0;
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
//    this.row = Bytes.readByteArray(in);
//    int familyMapSize = in.readInt();
//    this.familyMap = new HashMap<byte[],List<KeyValue>>();
//    byte[] family = null;
//    
//    List<KeyValue> list = null;
//    int listSize = 0;
//    KeyValue kv = null;
//    
//    for(int i=0; i<familyMapSize; i++){
//      family = Bytes.readByteArray(in);
//      listSize = in.readInt();
//      list = new ArrayList<KeyValue>(listSize);
//      for(int j=0; j<listSize; j++){
//        kv = new KeyValue();
//        kv.readFields(in);
//        list.add(kv);
//      }
//      this.familyMap.put(family, list);
//    }
//    this.rl = in.readLong();
  }  
  
  public void write(final DataOutput out)
  throws IOException {
//    Bytes.writeByteArray(out, this.row);
//    out.writeInt(familyMap.size());
//    List<KeyValue> tmpList = null;
//    for(Map.Entry<byte[], List<KeyValue>> entry : familyMap.entrySet()){
//      Bytes.writeByteArray(out, entry.getKey());
//      tmpList = entry.getValue();
//      out.writeInt(tmpList.size());
//      for(KeyValue kv : tmpList){
//        kv.write(out);
//      }
//    }
//    out.writeLong(this.rl);
  }
  
}

