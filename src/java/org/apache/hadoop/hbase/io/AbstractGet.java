package org.apache.hadoop.hbase.io;

import java.util.Set;

import org.apache.hadoop.hbase.filter.RowFilterInterface;

public abstract class AbstractGet implements Get {
  byte [] row = null;
  Family[] families = null;
  int versions = 0;
  TimeRange tr = null;
  RowFilterInterface filter = null;
  
//  public AbstractGet(){}
  
//  public AbstractGet(Get get){
//    this.row = get.getRow();
//    this.families = get.getFamilies();
//    this.tr = get.getTimeRange();
//  }

  @Override
  public Family[] getFamilies(){
    return this.families;
  }
  @Override
  public void setFamilies(Set<byte[]> families){
    int famLen = families.size();
    this.families = new Family[famLen]; 
    int i = 0;
    for(byte [] family  : families){
      this.families[i] = new Family(family);
      i++;
    }
  }
  
  @Override
  public RowFilterInterface getFilter(){
    return filter;
  }
  @Override
  public void setFilter(RowFilterInterface filter){
    this.filter = filter;
  }
  
  @Override
  public byte [] getRow(){
    return this.row;
  }
  
  @Override
  public TimeRange getTimeRange(){
    return tr;
  }
  
  @Override
  public int getVersions(){
    return this.versions;
  }
  @Override
  public void setVersions(int versions){
    this.versions = versions;
  }

}
