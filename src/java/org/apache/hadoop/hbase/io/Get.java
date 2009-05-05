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

import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Used to perform Get operations on a single row.
 * <p>
 * To get everything for a row, instantiate a Get object with the row to get.
 * To further define the scope of what to get, perform additional methods as 
 * outlined below.
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
public class Get implements Writable {
  private byte [] row = null;
  private long lockId = -1L;
  private int maxVersions = 1;
  private RowFilterInterface filter = null;
  private TimeRange tr = new TimeRange();
  private Map<byte [], Set<byte []>> familyMap = 
	new TreeMap<byte [], Set<byte []>>(Bytes.BYTES_COMPARATOR);
	  
  /** Constructor for Writable.  DO NOT USE */
  public Get() {}
  
  /**
   * Create a Get operation for the specified row.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   */
  public Get(byte [] row) {
	this(row, null);
  }
  
  /**
   * Create a Get operation for the specified row, using an existing row lock.
   * <p>
   * If no further operations are done, this will get the latest version of
   * all columns in all families of the specified row.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Get(byte [] row, RowLock rowLock) {
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
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
   * Get the column from the specific family with the specified qualifier.
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
  public void setMaxVersions(int maxVersions) throws IOException {
	if(maxVersions <= 0) {
	  throw new IOException("maxVersions must be positive");
	}
    this.maxVersions = maxVersions;
  }
  
  /**
   * Apply the specified server-side filter when performing the Get.
   * @param filter filter to run on the server
   */
  public void setFilter(RowFilterInterface filter) {
    this.filter = filter;
  }
  
  public Map<byte [], Set<byte []>> getFamilyMap() {
    return this.familyMap;
  }
  
  public byte [] getRow() {
    return this.row;
  }
  
  public RowLock getRowLock() {
	if(this.lockId == -1L) {
      return null;
	}
    return new RowLock(this.row, this.lockId);
  }
  
  public long getLockId() {
	return this.lockId;
  }
  
  public int getMaxVersions() {
    return this.maxVersions;
  } 

  public TimeRange getTimeRange() {
    return this.tr;
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
	this.row = Bytes.readByteArray(in);
	this.lockId = in.readLong();
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
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.lockId);
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
