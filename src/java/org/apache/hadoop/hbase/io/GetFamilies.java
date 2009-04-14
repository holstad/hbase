package org.apache.hadoop.hbase.io;

public class GetFamilies extends AbstractGet{
  
  public GetFamilies(byte [] row, byte [] family, byte versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = new Family[]{new Family(family)};
    super.tr = tr;
  }

}
