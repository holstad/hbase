package org.apache.hadoop.hbase.io;


import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;;


//TODO should be able to insert a list of KeyValue and get them sorted and
//inserted into the familyMap before sending to the server, should be done in
//commit
public class RowUpdates implements HeapSize{
  byte [] row = null;
  SortedMap<byte [] , List<KeyValue>> familyMap = null;
  
  private long rowLock = -1l;

  public RowUpdates(byte [] row, byte [] family, List<KeyValue> kvs){
    validateInput(row, family);
    familyMap = new TreeMap<byte [] , List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, kvs);
  }
  
  
  public SortedMap<byte [], List<KeyValue>> getFamilyMap(){
    return familyMap;
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
  
  
  //Checking if the length of row and family are within their bounds
  private void validateInput(byte [] row, byte [] family)
  throws IllegalArgumentException {
    if(row.length > HConstants.MAX_ROW_LENGTH ||
      family.length > HConstants.MAX_FAMILY_LENGTH){
      throw new IllegalArgumentException("Row or family are too long to fit into" +
      "a KeyValue, row.length " + row.length + " ecpected smaller" +
                "than " +HConstants.MAX_ROW_LENGTH+ " and family.length " +
                family.length + " expected smaller than " +
                HConstants.MAX_FAMILY_LENGTH);
    }
  }
}

