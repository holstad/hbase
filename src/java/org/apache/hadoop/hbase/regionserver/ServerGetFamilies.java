package org.apache.hadoop.hbase.regionserver;

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
  private List<byte[]> newColumns = null;

  private byte[] newColumn = null;
  private boolean newAdd = false; 
  
  public ServerGetFamilies(Get getFamilies){
    super(getFamilies);
  }

  @Override
  public int compareTo(KeyValue kv, boolean multiFamily) {
    
    
    // TODO Auto-generated method stub
    return 0;
  }

  //TODO These methods needs to be changed depending on the type of get you are
  //dealing with
 
  
  public List<byte[]> mergeGets(){
    
    return;
  }
  
  
  /**
   * This method are very different than the other compares, because it will
   * loop through the columns in the column list until the current kv is found
   * or the column is smaller than the kv.
   * 
   * The structure of the column is byte[int columnSize, byte[] column,
   * byte versions fetched]
   */
  protected int compareColumn(byte[] bytes, int offset, int length){
    if(columns == null){
      //Means that there are no columns in the current list, compare to the last
      //added column.
      
      
      //add to result and add to newColumns
      newColumns.add(key);
      
      return super.ADD;
      
    }
    
    if(super.columnIterator == null){
      
      super.columnIterator = columns.iterator();
      if(super.columnIterator.hasNext()){
        super.column = super.columnIterator.next();
      }
    }

    int res = 0;
    while(true){
      res = Bytes.compareTo(super.column, Bytes.SIZEOF_INT, Bytes.toInt(column),
          bytes, offset, length);
      if(res >= 0){
        return res;
      }
      if(super.columnIterator.hasNext()){
        super.column = super.columnIterator.next();
      } else {
        return res;
      }
    }
  }
  
  
  //Only check on the ts on the storefile to decide if we are done or not, or if 
  //this is the last storefile.
  private boolean isDone(){
    if(super.columns.isEmpty()){
      return true;
    }
    return false;
  }
  
  private int updateVersions(){
    int versionPos = super.column.length-1;
    byte versions = ++super.column[versionPos];
    if(versions >= super.getMaxVersions()){
//      super.columnIterator.remove();
    } else{
      super.column[versionPos] = versions;
    }
    return ;
  }
  
}
