package org.apache.hadoop.hbase.io;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class Update {
  protected byte[] row = null;
  protected long rowLock = -1;
  protected Map<byte[], List<KeyValue>> familyMap = 
    new TreeMap<byte[], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
  
  public byte[] getRow(){
    return this.row;
  }
  
  public long getRowLock(){
    return this.rowLock;
  }
  
  public Map<byte[], List<KeyValue>> getFamilyMap(){
    return this.familyMap;
  }
}
