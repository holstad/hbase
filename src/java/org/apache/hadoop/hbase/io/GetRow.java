package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class GetRow extends AbstractGet { // implements Get {

  public GetRow(byte [] row, byte versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = new Family[]{new Family()};
    super.tr = tr;
  }

  public void setFamilies(Set<byte []> families){
    int famSize = families.size();
    this.families = new Family[famSize];
    Iterator<byte[]> iter = families.iterator();
    int i=0;
    while(iter.hasNext()){
      this.families[i] = new Family(iter.next());
      i++;
    }
  }
  
}
