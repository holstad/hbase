package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.filter.RowFilterInterface;;

/**
 * Interface for all the different get calls 
 * 
 */
public interface Get extends Writable{

  public Family[] getFamilies();
  public void setFamilies(Family [] families);

  public RowFilterInterface getFilter();
  public void setFilter(RowFilterInterface filter);

  public byte [] getRow();
  
  public TimeRange getTimeRange();
  
  public byte getVersions();
  public void setVersions(byte versions);
  
  //Writable
  public void readFields(final DataInput in) throws IOException;
  public void write(final DataOutput out) throws IOException;
  
}
