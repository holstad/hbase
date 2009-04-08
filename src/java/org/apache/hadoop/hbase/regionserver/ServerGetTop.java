package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;

public class ServerGetTop extends AbstractServerGet {

  public ServerGetTop(Get get){
    super(get);
  }
  
  @Override
  public int compareTo(KeyValue kv, boolean multiFamily) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

}
