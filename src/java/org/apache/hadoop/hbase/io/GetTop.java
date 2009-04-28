package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class GetTop extends AbstractGet {
  private int fetches = 0;
  
  public GetTop(){}

  public GetTop(byte [] row, byte [] family, int fetches){
    this(row, family, fetches, new TimeRange());
  }
  public GetTop(byte [] row, byte [] family, int fetches, TimeRange tr){
    super.row = row;
    this.fetches = fetches;
    super.families.add(new Family(family));
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

  //Writable
  @Override
  public void readFields(final DataInput in) throws IOException {
    super.readFields(in);
    this.fetches = in.readInt();
  }
  
  @Override
  public void write(final DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(this.fetches);
  }
  
}
