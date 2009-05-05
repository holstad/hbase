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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Used to perform Put operations for a single row.
 * <p>
 * To perform a Put, instantiate a Put object with the row to insert to and
 * for each column to be inserted, execute {@link #add(byte[], byte[], byte[]) add} or
 * {@link #add(byte[], byte[], long, byte[]) add} if setting the timestamp.
 */
public class Put implements HeapSize, Writable {
  private byte [] row = null;
  private long timestamp = HConstants.LATEST_TIMESTAMP;
  private long lockId = -1L;
  private Map<byte [], List<KeyValue>> familyMap =
    new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

  /** Constructor for Writable.  DO NOT USE */
  public Put() {}

  /**
   * Create a Put operation for the specified row.
   * @param row row key
   */
  public Put(byte [] row) {
    this(row,null);
  }

  /**
   * Create a Put operation for the specified row, using an existing row lock.
   * @param row row key
   * @param rowLock previously acquired row lock, or null
   */
  public Put(byte [] row, RowLock rowLock) {
    this.row = row;
    if(rowLock != null) {
      this.lockId = rowLock.getLockId();
    }
  }

  /**
   * Copy constructor.  Creates a Put operation cloned from the specified Put.
   * @param putToCopy put to copy
   */
  public Put(Put putToCopy) {
    this(putToCopy.getRow(), putToCopy.getRowLock());
    this.familyMap = 
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(Map.Entry<byte [], List<KeyValue>> entry :
      putToCopy.getFamilyMap().entrySet()) {
      this.familyMap.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Add the specified column and value to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param value column value
   */
  public void add(byte [] family, byte [] qualifier, byte [] value) {
    add(family, qualifier, this.timestamp, value);
  }

  /**
   * Add the specified column and value, with the specified timestamp as 
   * its version to this Put operation.
   * @param family family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value column value
   */
  public void add(byte [] family, byte [] qualifier, long timestamp, byte [] value) {
    List<KeyValue> list = familyMap.get(family);
    if(list == null) {
      list = new ArrayList<KeyValue>();
    }
    KeyValue kv = new KeyValue(this.row, family, qualifier, timestamp, 
        KeyValue.Type.Put, value); 
    list.add(kv);
    familyMap.put(family, list);
  }

  public Map<byte [], List<KeyValue>> getFamilyMap() {
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

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Row ");
    sb.append(new String(this.row));
    sb.append(", Families[ ");
    for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      sb.append(new String(entry.getKey()));
      sb.append(", (");
      for(KeyValue kv : entry.getValue()) {
        sb.append(kv.toString());
        sb.append(", ");
      }
      sb.append("), ");
    }
    sb.append("]");    

    return sb.toString();
  }


  //HeapSize
  public long heapSize() {
    long totalSize = 0;
    for(Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      for(KeyValue kv : entry.getValue()) {
        totalSize += kv.heapSize();
      }
    }
    return totalSize;
  }

  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.row = Bytes.readByteArray(in);
    this.timestamp = in.readLong();
    this.lockId = in.readLong();
    int numFamilies = in.readInt();
    this.familyMap = 
      new TreeMap<byte [],List<KeyValue>>(Bytes.BYTES_COMPARATOR);
    for(int i=0;i<numFamilies;i++) {
      byte [] family = Bytes.readByteArray(in);
      int numKeys = in.readInt();
      List<KeyValue> keys = new ArrayList<KeyValue>(numKeys);
      for(int j=0;j<numKeys;j++) {
        KeyValue kv = new KeyValue();
        kv.readFields(in);
        keys.add(kv);
      }
      this.familyMap.put(family, keys);
    }
  }

  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(this.timestamp);
    out.writeLong(this.lockId);
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], List<KeyValue>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      List<KeyValue> keys = entry.getValue();
      out.writeInt(keys.size());
      for(KeyValue kv : keys) {
        kv.write(out);
      }
    }
  }
}
