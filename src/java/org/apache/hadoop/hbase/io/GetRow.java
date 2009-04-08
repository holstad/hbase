package org.apache.hadoop.hbase.io;

import java.util.Iterator;
import java.util.Set;

public class GetRow extends AbstractGet { // implements Get {

  public GetRow(byte [] row, int versions, TimeRange tr){
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
