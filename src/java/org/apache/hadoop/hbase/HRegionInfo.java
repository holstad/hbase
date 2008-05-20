/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.io.WritableComparable;

/**
 * HRegion information.
 * Contains HRegion id, start and end keys, a reference to this
 * HRegions' table descriptor, etc.
 */
public class HRegionInfo implements WritableComparable {
  /**
   * @param regionName
   * @return the encodedName
   */
  public static int encodeRegionName(final byte [] regionName) {
    return Math.abs(JenkinsHash.hash(regionName, regionName.length, 0));
  }

  /** delimiter used between portions of a region name */
  public static final int DELIMITER = ',';

  /** HRegionInfo for root region */
  public static final HRegionInfo ROOT_REGIONINFO =
    new HRegionInfo(0L, HTableDescriptor.ROOT_TABLEDESC);

  /** HRegionInfo for first meta region */
  public static final HRegionInfo FIRST_META_REGIONINFO =
    new HRegionInfo(1L, HTableDescriptor.META_TABLEDESC);

  /**
   * Extracts table name prefix from a region name.
   * Presumes region names are ASCII characters only.
   * @param regionName A region name.
   * @return The table prefix of a region name.
   */
  public static byte [] getTableNameFromRegionName(final byte [] regionName) {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1) {
      throw new IllegalArgumentException(Bytes.toString(regionName) +
        " does not contain '" + DELIMITER + "' character");
    }
    byte [] tableName = new byte[offset];
    System.arraycopy(regionName, 0, tableName, 0, offset);
    return tableName;
  }

  private byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
  private boolean offLine = false;
  private long regionId = -1;
  private byte [] regionName = HConstants.EMPTY_BYTE_ARRAY;
  private String regionNameStr = "";
  private boolean split = false;
  private byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
  private HTableDescriptor tableDesc = null;
  private int hashCode = -1;
  public static final int NO_HASH = -1;
  private volatile int encodedName = NO_HASH;
  
  private void setHashCode() {
    int result = this.regionName.hashCode();
    result ^= this.regionId;
    result ^= this.startKey.hashCode();
    result ^= this.endKey.hashCode();
    result ^= Boolean.valueOf(this.offLine).hashCode();
    result ^= this.tableDesc.hashCode();
    this.hashCode = result;
  }
  
  /**
   * Private constructor used constructing HRegionInfo for the catalog root and
   * first meta regions
   */
  private HRegionInfo(long regionId, HTableDescriptor tableDesc) {
    this.regionId = regionId;
    this.tableDesc = tableDesc;
    this.regionName = createRegionName(tableDesc.getName(), null, regionId);
    this.regionNameStr = Bytes.toString(this.regionName);
    setHashCode();
  }

  /** Default constructor - creates empty object */
  public HRegionInfo() {
    this.tableDesc = new HTableDescriptor();
  }
  
  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final HTableDescriptor tableDesc, final byte [] startKey,
      final byte [] endKey)
  throws IllegalArgumentException {
    this(tableDesc, startKey, endKey, false);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   * 
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, final byte [] startKey,
      final byte [] endKey, final boolean split)
  throws IllegalArgumentException {
    if (tableDesc == null) {
      throw new IllegalArgumentException("tableDesc cannot be null");
    }
    this.offLine = false;
    this.regionId = System.currentTimeMillis();
    this.regionName = createRegionName(tableDesc.getName(), startKey, regionId);
    this.regionNameStr = Bytes.toString(this.regionName);
    this.split = split;
    this.endKey = endKey == null? HConstants.EMPTY_END_ROW: endKey.clone();
    this.startKey = startKey == null?
      HConstants.EMPTY_START_ROW: startKey.clone();
    this.tableDesc = tableDesc;
    setHashCode();
  }
  
  private static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final long regionid) {
    return createRegionName(tableName, startKey, Long.toString(regionid));
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id.
   * @return Region name made of passed tableName, startKey and id
   */
  public static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final String id) {
    return createRegionName(tableName, startKey, Bytes.toBytes(id));
  }
  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id
   * @return Region name made of passed tableName, startKey and id
   */
  public static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final byte [] id) {
    byte [] b = new byte [tableName.length + 2 + id.length +
       (startKey == null? 0: startKey.length)];
    int offset = tableName.length;
    System.arraycopy(tableName, 0, b, 0, offset);
    b[offset++] = DELIMITER;
    if (startKey != null && startKey.length > 0) {
      System.arraycopy(startKey, 0, b, offset, startKey.length);
      offset += startKey.length;
    }
    b[offset++] = DELIMITER;
    System.arraycopy(id, 0, b, offset, id.length);
    return b;
  }
  
  /** @return the endKey */
  public byte [] getEndKey(){
    return endKey;
  }

  /** @return the regionId */
  public long getRegionId(){
    return regionId;
  }

  /**
   * @return the regionName as an array of bytes.
   * @see #getRegionNameAsString()
   */
  public byte [] getRegionName(){
    return regionName;
  }

  /**
   * @return Region name as a String for use in logging, etc.
   */
  public String getRegionNameAsString() {
    return this.regionNameStr;
  }
  
  /** @return the encoded region name */
  public synchronized int getEncodedName() {
    if (this.encodedName == NO_HASH) {
      this.encodedName = encodeRegionName(this.regionName);
    }
    return this.encodedName;
  }

  /** @return the startKey */
  public byte [] getStartKey(){
    return startKey;
  }

  /** @return the tableDesc */
  public HTableDescriptor getTableDesc(){
    return tableDesc;
  }
  
  /** @return true if this is the root region */
  public boolean isRootRegion() {
    return this.tableDesc.isRootRegion();
  }
  
  /** @return true if this is the meta table */
  public boolean isMetaTable() {
    return this.tableDesc.isMetaTable();
  }

  /** @return true if this region is a meta region */
  public boolean isMetaRegion() {
    return this.tableDesc.isMetaRegion();
  }
  
  /**
   * @return True if has been split and has daughters.
   */
  public boolean isSplit() {
    return this.split;
  }
  
  /**
   * @param split set split status
   */
  public void setSplit(boolean split) {
    this.split = split;
  }

  /**
   * @return True if this region is offline.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * @param offLine set online - offline status
   */
  public void setOffline(boolean offLine) {
    this.offLine = offLine;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "regionname: " + this.regionNameStr + ", startKey: <" +
      Bytes.toString(this.startKey) + ">, endKey: <" +
      Bytes.toString(this.endKey) + 
      ">, encodedName: " + getEncodedName() + "," +
      (isOffline()? " offline: true,": "") + (isSplit()? " split: true,": "") +
      " tableDesc: {" + this.tableDesc.toString() + "}";
  }
    
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return this.hashCode;
  }

  //
  // Writable
  //

  /**
   * {@inheritDoc}
   */
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, endKey);
    out.writeBoolean(offLine);
    out.writeLong(regionId);
    Bytes.writeByteArray(out, regionName);
    out.writeBoolean(split);
    Bytes.writeByteArray(out, startKey);
    tableDesc.write(out);
    out.writeInt(hashCode);
  }
  
  /**
   * {@inheritDoc}
   */
  public void readFields(DataInput in) throws IOException {
    this.endKey = Bytes.readByteArray(in);
    this.offLine = in.readBoolean();
    this.regionId = in.readLong();
    this.regionName = Bytes.readByteArray(in);
    this.regionNameStr = Bytes.toString(this.regionName);
    this.split = in.readBoolean();
    this.startKey = Bytes.readByteArray(in);
    this.tableDesc.readFields(in);
    this.hashCode = in.readInt();
  }
  
  //
  // Comparable
  //
  
  /**
   * {@inheritDoc}
   */
  public int compareTo(Object o) {
    HRegionInfo other = (HRegionInfo) o;
    if (other == null) {
      return 1;
    }
    
    // Are regions of same table?
    int result = this.tableDesc.compareTo(other.tableDesc);
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(this.startKey, other.startKey);
    if (result != 0) {
      return result;
    }
    
    // Compare end keys.
    return Bytes.compareTo(this.endKey, other.endKey);
  }
}
