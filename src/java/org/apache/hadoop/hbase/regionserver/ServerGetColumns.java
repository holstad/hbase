package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.util.Bytes;


public class ServerGetColumns extends AbstractServerGet {
  
//  private List<Key> newDeletes = new ArrayList<Key>();

  private List<byte[]> columnsToDelete = new ArrayList<byte[]>();
  
  private static final int KEY_OFFSET = 2*Bytes.SIZEOF_INT;

  private boolean outOfTimeRange = false;
  
//  private static final int NEXT_KV = 0;
//  private static final int ADD = 1;
//  private static final int NEXT_SF = 2;
//  private static final int DONE = 3;
  
  public ServerGetColumns(Get get){
    super(get);
  }
  
//  public List<Key> getNewDeletes(){
//    return newDeletes;
//  }  
  

  /**
   * @param kv, the KeyValue to compare with
   * @return int the return code 
   */
//  public int compareTo(KeyValue kv){
//    return compareTo(kv, false);
//  }
  
  public int compareTo(KeyValue kv, boolean multiFamily){
    if(isDone()){
      return super.DONE;
    }
    
    int initialOffset = kv.getOffset();
    int offset = initialOffset;
    byte [] bytes = kv.getBuffer();

    //Getting key length
    int keyLen = Bytes.toInt(bytes, offset);
    offset += Bytes.SIZEOF_INT;

    //Skipping valueLength
    offset += Bytes.SIZEOF_INT;

    //Getting row length
    short rowLen = Bytes.toShort(bytes, offset);
    offset += Bytes.SIZEOF_SHORT;

    byte [] row = super.getRow();
    int ret = Bytes.compareTo(row, 0, row.length, bytes, offset, rowLen);
    if(ret <= -1){
      if(outOfTimeRange){
        return super.DONE;
      }
      return super.NEXT_SF;
    } else if(ret >= 1){
      return super.NEXT_KV; 
    }
    offset += rowLen;

    //This is only for the future if we have more than on family in the same
    //storefile, can be turned off for now
    //Getting family length
    byte famLen = bytes[offset];
    offset += Bytes.SIZEOF_BYTE;

    if(multiFamily){
      byte [] family = super.getFamily();
      ret = Bytes.compareTo(family, 0, family.length, bytes, offset, rowLen);
      if(ret <= -1){
        if(outOfTimeRange){
          return super.DONE;
        }
        return super.NEXT_SF;
      } else if(ret >= 1){
        return super.NEXT_KV; 
      }
    }
    offset += famLen;

    //Getting column length
    int colLen = KEY_OFFSET + keyLen - (offset-initialOffset) - 
    Bytes.SIZEOF_LONG - Bytes.SIZEOF_BYTE;
    //Could be switched to something like:
    //initloffset + keylength - TIMESTAMP_TYPE_SIZE - loffset;
    
    //Checking TTL and TimeRange before column so that if the entry turns out
    //to be a delete we can be sure that all the deletes in the delete list
    //are valid deletes
    int tsOffset = offset + colLen;
    long ts = Bytes.toLong(bytes, tsOffset);
    ret = super.checkTTL(ts);
    if(ret == 0){
      return super.NEXT_KV;
    }

    ret = super.get.getTimeRange().withinTimeRange(ts);
    if(ret != 1){
      return super.NEXT_KV;
    }

    //Check if kv is a delete
    if(super.isDelete(bytes, initialOffset, keyLen)){
      super.newDeletes.add(kv);
      return super.NEXT_KV;
    }     

    //There is not going to be any ts in here, but all tss will be taken care
    //of in the TimeRange check
    ret = compareColumn(bytes, offset, colLen);
    if(ret <= -1){
      return super.NEXT_SF;
    } else if(ret >= 1){
      return super.NEXT_KV;
    }
    offset += colLen;

    if(!super.deletes.isEmpty()){
      ret = super.isDeleted(bytes, initialOffset, rowLen, famLen,
          colLen, multiFamily);
      if(ret != 0){
        if(ret >= 1){
          outOfTimeRange = true;
        }
        return super.NEXT_KV;
      }
    }
    
    //TODO
    //ret = compareFilter(kv);

    //Includes remove from getList, TODO change for other calls
    return updateVersions();
  }
  
/*******************************************************************************
* Helpers 
*******************************************************************************/  
  //TODO These methods needs to be changed depending on the type of get you are
  //dealing with
  
  private boolean isDone(){
    int len = super.columns.size();
    if(super.columns.isEmpty()){
      return true;
    }
    return false;
  }

  
  /**
   * This method are very different than the other compares, because it will
   * loop through the columns in the column list until the current kv is found
   * or the column is smaller than the kv.
   * 
   */
  private int compareColumn(byte[] bytes, int offset, int length){
    if(columns != null){
      if(super.column == null){
        super.column = super.columns.get(super.columnPos);
      }
    }
    int res = 0;
    while(true){
      res = Bytes.compareTo(column.getBuffer(), column.getOffset(),
          column.getLength(), bytes, offset, length);
      if(res >= 0){
        return res;
      }
//      int size = columns.size();
      if(columnPos < columns.size()-1){
        column = columns.get(super.columnPos++);
      } else {
        return res;
      }
    }
  }
  
  
  
  
  private int updateVersions(){
    short version = 0;
    int size = 0;
    try{
      size = super.versions.size();
      version = super.versions.get(super.columnPos);
    } catch (Exception e){
      super.versions.add(version);
      size = super.versions.size();
//      return super.ADD;
    }
    int pos = super.columnPos;
    version++;
    if(version >= super.getMaxVersions()){
      //Remove this position from list, for now, might do something else later
      super.columns.remove(super.columnPos);
    } else {
      ((ArrayList<Short>)super.versions).set(super.columnPos, version);
    }
    size = super.versions.size();
    return super.ADD;
  }
 
  
}
