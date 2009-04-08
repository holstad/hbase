package org.apache.hadoop.hbase.io;

public class GetTop extends AbstractGet {
  private int fetches = 0;
  
  public GetTop(byte [] row, byte [] family, int fetches, TimeRange tr){
    super.row = row;
    this.fetches = fetches;
    super.families = new Family[]{new Family(family)};
    super.tr = tr;
  }
  
  public int getFetches(){
    return fetches;
  }
  public void setFetches(int fetches){
    this.fetches = fetches;
  }
  public int incFetchesAndReturn(){
    return ++fetches;
  }

}
