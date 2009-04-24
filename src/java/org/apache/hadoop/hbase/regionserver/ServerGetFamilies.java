package org.apache.hadoop.hbase.regionserver;

//import java.io.IOException;
//import java.util.SortedSet;
//import java.util.TreeSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.regionserver.Key;

//import org.apache.hadoop.hbase.io.GetColumns;
//import org.apache.hadoop.hbase.io.GetFamilies;
import org.apache.hadoop.hbase.util.Bytes;

public class ServerGetFamilies extends AbstractServerGet{
  
//  private List<byte[]> newColumns = null;
//  private List<KeyValue> newColumns = null;
//  private List<Short> newVersions = null; 
//
//  private KeyValue newColumn = null;
//  private boolean endColumns = false; 
  
  public ServerGetFamilies(Get getFamilies){
    super(getFamilies);
    super.newColumns = new ArrayList<KeyValue>();
    super.newVersions = new ArrayList<Short>();
  }

  public List<KeyValue> getNewColumns(){
    return newColumns;
  }
  
  @Override
  public int compareTo(KeyValue kv, boolean multiFamily) {
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
        return super.NEXT_SF;
      } else if(ret >= 1){
        return super.NEXT_KV; 
      }
    }
    offset += famLen;

    //Getting column length
    int colLen = super.KEY_OFFSET + keyLen - (offset-initialOffset) - 
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
    //TODO
    //ret = compareFilter(kv);
    
    //Check if this kv is deleted
    if(!super.deletes.isEmpty()){
      ret = super.isDeleted(bytes, initialOffset, rowLen, famLen,
          colLen, multiFamily);
      if(ret != 0){
        return super.NEXT_KV;
      }
    }
    //There is not going to be any ts in here, but all tss will be taken care
    //of in the TimeRange check
    ret = compareColumn(bytes, offset, colLen);

    //Includes remove from getList, TODO change for other calls
    return updateVersions(ret);
  }

  //TODO These methods needs to be changed depending on the type of get you are
  //dealing with
 
  
//  public List<KeyValue> mergeGets(boolean multiFamily){
//  public void mergeGets(boolean multiFamily){
//    int oldPos = 0;
//    int newPos = 0;
//    int oldSize = super.columns.size();
//    int newSize = newColumns.size();
//    System.out.println("oldSize " +oldSize);
//    System.out.println("newSize " +newSize);
//    
//    int size =  oldSize + newSize;
//    List<KeyValue> mergedList = new ArrayList<KeyValue>(size);
//    List<Short> mergedVersions = new ArrayList<Short>(size);
//    
//    if(oldSize == 0){
//      reinit();
//      return;
//    }
//    if(newSize == 0){
//      return;
//    }
//    
//    int res = 0;
//    KeyValue newe = null;
//    boolean newDone = false;
//    KeyValue olde = null;
//    while(true){
//      newe = newColumns.get(newPos);
//      olde = super.columns.get(oldPos);
//      res = Bytes.compareTo(newe.getBuffer(), newe.getOffset(), newe.getLength(),
//          olde.getBuffer(), olde.getOffset(), olde.getLength());
//      if(res <= -1){
//        mergedList.add(newe);
//        mergedVersions.add(newVersions.get(newPos));
//        if(++newPos >= newSize){
//          newDone = true;
//          break;
//        }
//      } else if(res >= 1){
//        mergedList.add(olde);
//        mergedVersions.add(newVersions.get(oldPos));
//        if(++oldPos >= oldSize){
//          break;
//        }        
//      } else {
//        while(true){
//          System.out.println("Error!! Same value int old and new, in" +
//          "mergeGets");
//        }
//      }
//    }
//    if(newDone){
//      while(oldPos < oldSize){
//        mergedList.add(super.columns.get(oldPos));
//        mergedVersions.add(newVersions.get(oldPos++));
//      }
//    } else {
//      while(newPos < newSize){
//        mergedList.add(newColumns.get(newPos));
//        mergedVersions.add(newVersions.get(newPos++));
//      }
//    }
//    reinit();
//  }
  
  /**
   * 
   * 
   * @param bytes
   * @param offset
   * @param length
   * @return -1 if this column cannot be found in newColumn nor columns, 0 if 
   * this as the same as newColumn and 1 if the same as column
   */
  private int compareColumn(byte[] bytes, int offset, int length) {
    int res = 0;
    
    if(newColumn != null){
      res = Bytes.compareTo(newColumn.getBuffer(), newColumn.getOffset(),
          newColumn.getLength(), bytes, offset, length);
      if(res == 0){
        return 0;
      }
      else if(res >= 1){
        for(int i=0; i<10; i++){
          System.out.println("Error in compareColumn, res " +res);
        }
      }
    }

    if(!endColumns){
      if(columns != null && columns.size() > 0){
        if(super.column == null){
          super.column = super.columns.get(super.columnPos);
        }

        while(true){
          res = Bytes.compareTo(super.column.getBuffer(), super.column.getOffset(),
              super.column.getLength(), bytes, offset, length);
          //so when the entry we are looking at at the moment is smaller than 
          // column we know that it is not in there , so add to newColumns
          if(res >= 1 ){
            break;
          } else  if(res == 0){
            return 1;
          } else {
            if(super.columnPos < super.columns.size()-1){
              super.column = super.columns.get(++super.columnPos);
            } else {
              endColumns = true;
              break;
            }
          }
        }
      }
    }
    newColumn = new KeyValue(bytes, offset, length);
    newColumns.add(newColumn);
    return -1;
  }
  
  //if storefile header time is older than oldest time asked for in TimeRange
  //then abort or it this is the last storefile
  private boolean isDone(){
    return false;
  }
  
  private int updateVersions(int returnCode){
    if(returnCode == -1){
      newVersions.add((short)1);
//      System.out.println("adding a new entry to list, size " + newColumns.size());
      return super.ADD;
    } else if(returnCode == 0){
      int pos = newVersions.size();
      short value = newVersions.get(pos);
      if(++value > super.getMaxVersions()){
        return super.NEXT_KV;
      }
      newVersions.set(pos, value);
      return super.ADD;
    } else {
      short value = super.versions.get(columnPos);
      if(++value > super.getMaxVersions()){
        return super.NEXT_KV;
      }
      super.versions.set(super.columnPos, value);
      return super.ADD;
    }
  }
  
}
