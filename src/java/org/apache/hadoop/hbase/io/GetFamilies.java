package org.apache.hadoop.hbase.io;

public class GetFamilies extends AbstractGet{
  public GetFamilies(byte [] row, byte [] family, short versions){
    this(row, family, versions, new TimeRange());
  }
  
  public GetFamilies(byte [] row, byte [] family, short versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family(family));
    if(tr == null){
      super.tr = new TimeRange();
    }
  }

}
