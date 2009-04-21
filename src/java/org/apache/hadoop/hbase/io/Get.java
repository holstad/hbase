package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.filter.RowFilterInterface;;

/**
 * Interface for all the different get calls 
 * 
 */
public interface Get extends Writable{

  public List<Family> getFamilies();
  public void setFamilies(List<Family> families);

  public RowFilterInterface getFilter();
  public void setFilter(RowFilterInterface filter);

  public byte [] getRow();
  
  public TimeRange getTimeRange();
  
  public short getVersions();
  public void setVersions(short versions);
  
  //Writable
  public void readFields(final DataInput in) throws IOException;
  public void write(final DataOutput out) throws IOException;
  
}
