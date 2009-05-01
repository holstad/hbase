package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

///**
//* Interface for all the different get calls 
//* 
//*/
//public interface Get extends Writable{

//public List<Family> getFamilies();
//public void setFamilies(List<Family> families);

//public RowFilterInterface getFilter();
//public void setFilter(RowFilterInterface filter);

//public byte [] getRow();

//public TimeRange getTimeRange();

//public short getVersions();
//public void setVersions(short versions);

////Writable
//public void readFields(final DataInput in) throws IOException;
//public void write(final DataOutput out) throws IOException;

//}

public class Get implements Writable{
  private byte[] row = null;
  private Map<byte[], Set<byte[]>> familyMap =
    new HashMap<byte[], Set<byte[]>>();
  
    
  private long rowLock = 0L;
  private int maxVersions = 0;
  private RowFilterInterface filter = null;
  private TimeRange tr = new TimeRange();
  
  /**
   * Don't use this constructor, only for use with serialization
   */
  public Get(){}
  
  public Get(byte[] row){
    this.row = row;
  }
  public Get(byte[] row, long rowLock){
    this.row = row;
    this.rowLock = rowLock;
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
  
  public void setTimeStamp(long timestamp)
  throws IOException {
    tr = new TimeRange(timestamp, timestamp);
  }
  
  public void setMaxVersions(int maxVersions){
    this.maxVersions = maxVersions;
  }
  
  public void setFilter(RowFilterInterface filter){
    this.filter = filter;
  }
  
  
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.row = Bytes.readByteArray(in);
    int familyMapSize = in.readInt();
    this.familyMap = new HashMap<byte[], Set<byte[]>>();
    byte[] family = null;
    
    Set<byte[]> set = null;
    int listSize = 0;
    KeyValue kv = null;
    
    for(int i=0; i<familyMapSize; i++){
      family = Bytes.readByteArray(in);
      listSize = in.readInt();
      list = new ArrayList<KeyValue>(listSize);
      for(int j=0; j<listSize; j++){
        kv = new KeyValue();
        kv.readFields(in);
        list.add(kv);
      }
      this.familyMap.put(family, list);
    }

    this.rowLock = in.readLong();
    this.maxVersions = in.readInt();
//    filter.write(out);
    this.tr = new TimeRange();
    tr.readFields(in);
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeInt(familyMap.size());
    Set<byte[]> tmpSet = null;
    for(Map.Entry<byte[], Set<byte[]>> entry : familyMap.entrySet()){
      Bytes.writeByteArray(out, entry.getKey());
      tmpSet = entry.getValue();
      out.writeInt(tmpSet.size());
      for(byte[] bs : tmpSet){
        Bytes.writeByteArray(out, bs);
      }
    }
    out.writeLong(this.rowLock);
    out.writeInt(this.maxVersions);
//    filter.write(out);
    tr.write(out);
  }
  
}
