package org.apache.hadoop.hbase.regionserver;

import java.util.List;

/**
 * Class that holds a list of deletes and a timestamp of a DeleteFamily entry
 * The DeleteFamily timestamp is 0 if there is now such entry in the list 
 *
 */
public class Deletes {
  List<Key> deletes = null;
  long deleteFamily = 0L;
  
  /**
   * 
   * @param deletes
   * @param deleteFamily
   */
  public Deletes(List<Key> deletes, long deleteFamily){
    this.deletes = deletes;
    this.deleteFamily = deleteFamily;
  }

//  public boolean hasDeleteFamily(){
//    return deleteFamily != 0;
//  }
  
  /**
   * @return the DeleteFamily timestamp, 0 if no such entry
   */
  public long getDeleteFamily(){
    return deleteFamily;
  }
  
  /**
   * 
   * @return the list of deletes
   */
  public List<Key> getDeletes(){
    return deletes;
  }
  
  
}
