package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.util.Bytes;


public class ServerGetColumns extends AbstractServerGet {
  
  private List<Key> newDeletes = new ArrayList<Key>();

  private List<byte[]> columnsToDelete = new ArrayList<byte[]>();
  
  private static final int KEY_OFFSET = 2*Bytes.SIZEOF_INT;

  private boolean outOfTimeRange = false;
  
  private final int NEXT_KV = 0;
  private final int ADD = 1;
  private final int NEXT_SF = 2;
  private final int DONE = 3;
  
  public ServerGetColumns(Get get){
    super(get);
  }
  
  public List<Key> getNewDeletes(){
    return newDeletes;
  }  
  

  /**
   * @param kv, the KeyValue to compare with
   * @return int the return code 
   */
//  public int compareTo(KeyValue kv){
//    return compareTo(kv, false);
//  }
  
  public int compareTo(KeyValue kv, boolean multiFamily){
    if(isDone()){
      return DONE;
    }
    
    final boolean HAVE_DELETES = !deletes.isEmpty();

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
        return DONE;
      }
      return NEXT_SF;
    } else if(ret >= 1){
      return NEXT_KV; 
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
          return DONE;
        }
        return NEXT_SF;
      } else if(ret >= 1){
        return NEXT_KV; 
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
      return NEXT_KV;
    }

    ret = super.get.getTimeRange().withinTimeRange(ts);
    if(ret != 1){
      return NEXT_KV;
    }

    //Check if kv is a delete
    if(super.isDelete(bytes, initialOffset, keyLen)){
      newDeletes.add(new Key(kv));
      return NEXT_KV;
    }     

    //There is not going to be any ts in here, but all tss will be taken care
    //of in the TimeRange check
    ret = super.compareColumn(bytes, offset, colLen);
    if(ret <= -1){
      return NEXT_SF;
    } else if(ret >= 1){
      return NEXT_KV;
    }
    offset += colLen;

    if(HAVE_DELETES){
      ret = super.isDeleted(bytes, initialOffset, rowLen, famLen,
          colLen, multiFamily);
      if(ret != 0){
        if(ret >= 1){
          outOfTimeRange = true;
        }
        return NEXT_KV;
      }
    }
    
    //TODO
    //ret = compareFilter(kv);

    //Includes remove from getList, TODO change for other calls
    updateVersions();

    return ADD;
  }
  
/*******************************************************************************
* Helpers 
*******************************************************************************/  
  private boolean isDone(){
    if(super.columns.isEmpty()){
      return true;
    }
    return false;
  }
  
  
//TODO this is one method that needs to be changed for the other gets
  private void updateVersions(){
    int versionPos = super.column.length-1;
    byte versions = ++super.column[versionPos];
    if(versions >= super.getMaxVersions()){
      super.columnIterator.remove();
    } else{
      super.column[versionPos] = versions;
    }
  }
 
  
}
