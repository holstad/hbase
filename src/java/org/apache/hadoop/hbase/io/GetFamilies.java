package org.apache.hadoop.hbase.io;

public class GetFamilies extends AbstractGet{
  
  public GetFamilies(byte [] row, byte [] family, short versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family(family));
    super.tr = tr;
  }

}
