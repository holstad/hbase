package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class GetRow extends AbstractGet { // implements Get {

  public GetRow(byte [] row, short versions){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family());
    super.tr = new TimeRange();
  }
  
  public GetRow(byte [] row, short versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family());
    super.tr = tr;
  }

  public void setFamilies(Set<byte[]> families){
    int famSize = families.size();
    for(byte[] family : families){
      super.families.add(new Family(family));
    }
  }
  
}
