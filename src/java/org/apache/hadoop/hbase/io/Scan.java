/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.io.Writable;

/**
 * Used to perform Scan operations.
 * <p>
 * All operations are identical to {@link Get} with the exception of
 * instantiation.  Rather than specifying a single row, an optional startRow
 * and stopRow may be defined.  If rows are not specified, the Scanner will
 * iterate over all rows.
 * <p>
 * To scan everything for each row, instantiate a Scan object.
 * To further define the scope of what to get when scanning, perform additional 
 * methods as outlined below.
 * <p>
 * To get all columns from specific families, execute {@link #addFamily(byte[]) addFamily}
 * for each family to retrieve.
 * <p>
 * To get specific columns, execute {@link #addColumn(byte[], byte[]) addColumn}
 * for each column to retrieve.
 * <p>
 * To only retrieve columns within a specific range of version timestamps,
 * execute {@link #setTimeRange(long, long) setTimeRange}.
 * <p>
 * To only retrieve columns with a specific timestamp, execute
 * {@link #setTimeStamp(long) setTimestamp}.
 * <p>
 * To limit the number of versions of each column to be returned, execute
 * {@link #setMaxVersions(int) setMaxVersions}.
 * <p>
 * To add a filter, execute {@link #setFilter(RowFilterInterface) setFilter}.
 */
public class Scan implements Writable{
  private byte [] startRow = HConstants.EMPTY_START_ROW;
  private byte [] stopRow  = HConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private RowFilterInterface filter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], Set<byte []>> familyMap =
    new TreeMap<byte [], Set<byte []>>(Bytes.BYTES_COMPARATOR);
  
  /**
   * Create a Scan operation across all rows.
   */
  public Scan() {}
  
  /**
   * Create a Scan operation starting at the specified row.
   * <p>
   * If the specified row does not exist, the Scanner will start from the
   * next closest row after the specified row.
   * @param startRow row to start scanner at or after
   */
  public Scan(byte [] startRow) {
    this.startRow = startRow;
  }
  
  /**
   * Create a Scan operation for the range of rows specified.
   * @param startRow row to start scanner at or after (inclusive)
   * @param stopRow row to stop scanner before (exclusive)
   */
  public Scan(byte [] startRow, byte [] stopRow) {
    this.startRow = startRow;
    this.stopRow = stopRow;
  }
  
  /**
   * Get all columns from the specified family.
   * <p>
   * Overrides previous calls to addColumn for this family.
   * @param family family name
   */
  public void addFamily(byte [] family) {
    familyMap.remove(family);
    familyMap.put(family, null);
  }
  
  /**
   * Get the column from the specified family with the specified qualifier.
   * <p>
   * Overrides previous calls to addFamily for this family.
   * @param family family name
   * @param qualifier column qualifier
   */
  public void addColumn(byte [] family, byte [] qualifier) {
    Set<byte []> set = familyMap.get(family);
    if(set == null) {
      set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    }
    set.add(qualifier);
    familyMap.put(family, set);
  }
  
  /**
   * Get versions of columns only within the specified timestamp range,
   * [minStamp, maxStamp).
   * @param minStamp minimum timestamp value, inclusive
   * @param maxStamp maximum timestamp value, exclusive
   * @throws IOException if invalid time range
   */
  public void setTimeRange(long maxStamp, long minStamp)
  throws IOException {
    tr = new TimeRange(maxStamp, minStamp);
  }
  
  /**
   * Get versions of columns with the specified timestamp.
   * @param timestamp version timestamp  
   */
  public void setTimeStamp(long timestamp) {
    tr = new TimeRange(timestamp);
  }

  /**
   * Get all available versions.
   */
  public void setMaxVersions() {
	this.maxVersions = Integer.MAX_VALUE;
  }

  /**
   * Get up to the specified number of versions of each column.
   * @param maxVersions maximum versions for each column
   * @throws IOException if invalid number of versions
   */
  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
  }
  
  /**
   * Apply the specified server-side filter when performing the Scan.
   * @param filter filter to run on the server
   */
  public void setFilter(RowFilterInterface filter) {
    this.filter = filter;
  }
  
  public void setFamilyMap(Map<byte [], Set<byte []>> familyMap) {
    this.familyMap = familyMap;
  }
  
  public Map<byte [], Set<byte []>> getFamilyMap() {
    return this.familyMap;
  }
  
  public byte [] getStartRow() {
    return this.startRow;
  }

  public byte [] getStopRow() {
    return this.stopRow;
  }
  
  public int getMaxVersions() {
    return this.maxVersions;
  } 

  public TimeRange getTimeRange() {
    return this.tr;
  } 
  
  public RowFilterInterface getFilter() {
    return filter;
  }
  
  
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.startRow = Bytes.readByteArray(in);
    this.stopRow = Bytes.readByteArray(in);
	this.maxVersions = in.readInt();
	boolean hasFilter = in.readBoolean();
	if(hasFilter) {
	  this.filter = (RowFilterInterface)HbaseObjectWritable.readObject(in, null);
	}
	this.tr = new TimeRange();
	tr.readFields(in);
	int numFamilies = in.readInt();
    this.familyMap = 
      new TreeMap<byte [], Set<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      Set<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      for(int j=0; j<numColumns; j++) {
        byte [] qualifier = Bytes.readByteArray(in);
        set.add(qualifier);
      }
      this.familyMap.put(family, set);
    }
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.startRow);
    Bytes.writeByteArray(out, this.stopRow);
    out.writeInt(this.maxVersions);
    if(this.filter == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      HbaseObjectWritable.writeObject(out, this.filter, RowFilterInterface.class, null);
    }
    tr.write(out);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], Set<byte []>> entry : familyMap.entrySet()) {
	  Bytes.writeByteArray(out, entry.getKey());
	  Set<byte []> columnSet = entry.getValue();
	  out.writeInt(columnSet.size());
	  for(byte [] qualifier : columnSet) {
	    Bytes.writeByteArray(out, qualifier);
	  }
	}
  }
}
