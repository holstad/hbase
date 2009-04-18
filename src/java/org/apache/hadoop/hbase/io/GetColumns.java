package org.apache.hadoop.hbase.io;

import java.io.IOException; 
import java.util.List;

import org.apache.hadoop.hbase.util.Writables;

public class GetColumns extends AbstractGet{

  public GetColumns(byte [] row, byte [] family, byte [] column, byte versions,
    TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family(family, column));
    if (tr == null){
      tr = new TimeRange();
    }
    super.tr = tr;
  }

  public GetColumns(byte [] row, byte [] family, List<byte[]> columns,
      byte versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families.add(new Family(family, columns));
    if (tr == null){
      tr = new TimeRange();
    }
    super.tr = tr;
  }
  
  public GetColumns(byte [] row, List<Family> families, byte versions,
    TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = families;
    super.tr = tr;
  }

  public GetColumns(byte [] row, List<Family> families, byte versions)
  throws IOException{
    this(row, families, versions, System.currentTimeMillis());
  }
  
  public GetColumns(byte [] row, List<Family> families, byte versions,
    long ts)
  throws IOException{
    super.row = row;
    super.versions = versions;
    super.families = families;
    super.tr = new TimeRange(ts, ts);
  }
  
}
