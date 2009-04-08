package org.apache.hadoop.hbase.regionserver;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.GetFamilies;

public class ServerGetFamilies extends AbstractServerGet{
  
  public ServerGetFamilies(Get getFamilies){
    super(getFamilies);
  }

  @Override
  public int compareTo(KeyValue kv, boolean multiFamily) {
    // TODO Auto-generated method stub
    return 0;
  }

}
