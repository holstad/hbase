package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractGet implements Get {
  byte [] row = new byte [0];
//  Family [] families = new Family [0];
  List<Family> families = new ArrayList<Family>();
  //if this is changed to support more versions than this it also needs to be
  //changed in the family class and in the updates of the versions in the 
  //serverGets
  short versions = 0;
  TimeRange tr = new TimeRange();
  RowFilterInterface filter = null;
  

  @Override
  public List<Family> getFamilies(){
    return this.families;
  }
  @Override
  public void setFamilies(List<Family> families){
    this.families = families;
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
  public short getVersions(){
    return this.versions;
  }
  @Override
  public void setVersions(short versions){
    this.versions = versions;
  }
  
  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();

    sb.append("Row ");
    sb.append(new String(this.row));
    sb.append(", families [");
    int i = 0;
    for(; i<families.size()-1; i++){
      sb.append(families.get(i).toString());
      sb.append(", ");      
    }

    sb.append(new String(families.get(i).toString()));
    sb.append("]"); 
    return sb.toString();
  }
  
  //Writable
  public void readFields(final DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    int famLen = in.readInt();
    this.families = new ArrayList<Family>(famLen);
    for(int i=0; i<famLen; i++){
      Family family = new Family();
      family.readFields(in);
      this.families.add(family);
    }
    this.versions = in.readByte();
    tr = new TimeRange();
    tr.readFields(in);
    //TODO read in rowFilter
  }
  
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeInt(families.size());
    for(Family family : families){
      family.write(out);
    }
    out.writeByte(versions);
    tr.write(out);
    //TODO write rowFilter
  }
  
}
