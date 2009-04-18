package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


//TODO should be able to insert a list of KeyValue and get them sorted and
//inserted into the familyMap before sending to the server, should be done in
//commit
public class RowUpdates implements Writable, HeapSize{
  byte [] row = null;
//  SortedMap<byte [] , List<KeyValue>> familyMap = null;
  List<Family> families = new ArrayList<Family>();
  
  //TODO check if you want to get the timestamp of the send and not the time
  //when the Object is created
  private long ts = HConstants.LATEST_TIMESTAMP;
  private long rowLock = -1L;

  public RowUpdates(byte [] row, byte [] family, byte[] column){
    this.row = row;
    this.families.add(new Family(family, column));
  }
  
  public RowUpdates(byte [] row, byte [] family, List<byte[]> columns){
    this.row = row;
    this.families.add(new Family(family, columns));
  }

  public RowUpdates(byte [] row, byte [] family, List<byte[]> columns, long ts){
    this.row = row;
    this.families.add(new Family(family, columns));
    this.ts = ts;
  }

  public RowUpdates(byte [] row, Family Family, long ts){
    this.row = row;
    this.families.add(Family);
    this.ts = ts;
  }
  
  public RowUpdates(byte [] row, List<Family> putFamilies, long ts){
    this.row = row;
    this.families = putFamilies;
    this.ts = ts;
  }
  
  public void add(Family Family){
    this.families.add(Family);
  }
  public void add(List<Family> putFamilies){
    this.families.addAll(putFamilies);
  }
  
  public List<Family> getFamilies(){
    return families;
  }
  
  public byte [] getRow(){
    return row;
  }
  
  /**
   * Get the row lock associated with this update
   * @return the row lock
   */
  public long getRowLock() {
    return rowLock;
  }

  /**
   * Set the lock to be used for this update
   * @param rowLock the row lock
   */
  public void setRowLock(long rowLock) {
    this.rowLock = rowLock;
  }
  
  //TODO fix heapSize
  public long heapSize(){
    //Should be 16 + 24+row.length/8 + familyMap.heapSize + 8.
    //familyMap.heapSize 
    return 0;
  }
  
  public void createKeyValuesFromColumns(){
    for(Family family : families){
      family.createKeyValuesFromColumns(row, ts);
    }
  }
  
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.row = Bytes.readByteArray(in);
    int nFamilies = in.readInt();
    this.families = new ArrayList<Family>(nFamilies);
    for(int i=0; i<nFamilies; i++){
      Family family = new Family();
      family.readFields(in);
      families.add(family);
    }
    this.rowLock = in.readLong();
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeInt(families.size());
    for(Family family : families){
      family.write(out);
    }
    out.writeLong(this.rowLock);
  }  
  
}

