package org.apache.hadoop.hbase.io;

import java.io.IOException; 

public class GetColumns extends AbstractGet{

  /**
   * 
   * @param row
   * @param family
   * @param column
   * @param versions the maximum number of versions to be returned
   * @param tr
   */
  public GetColumns(byte [] row, byte [] family, byte [] column, int versions,
    TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = new Family[]{new Family(family, column)};
    if (tr == null){
      
    }
    super.tr = tr;
  }

  /**
   * 
   * @param row
   * @param family
   * @param column
   * @param versions the maximum number of versions to be returned
   * @param tr
   */
  public GetColumns(byte [] row, Family [] families, int versions,
    TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = families;
    super.tr = tr;
  }

  public GetColumns(byte [] row, Family [] families, int versions)
  throws IOException{
    this(row, families, versions, System.currentTimeMillis());
  }
  
  public GetColumns(byte [] row, Family [] families, int versions,
    long ts)
  throws IOException{
    super.row = row;
    super.versions = versions;
    super.families = families;
    super.tr = new TimeRange(ts, ts);
  }


  
}
