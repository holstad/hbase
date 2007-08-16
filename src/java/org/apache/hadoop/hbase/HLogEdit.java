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

import org.apache.hadoop.io.*;

import java.io.*;

/**
 * A log value.
 *
 * These aren't sortable; you need to sort by the matching HLogKey.
 * The table and row are already identified in HLogKey.
 * This just indicates the column and value.
 */
public class HLogEdit implements Writable {
  private Text column = new Text();
  private byte [] val;
  private long timestamp;
  private final int MAX_VALUE_LEN = 128;

  /**
   * Default constructor used by Writable
   */
  public HLogEdit() {
    super();
  }

  /**
   * Construct a fully initialized HLogEdit
   * @param column column name
   * @param bval value
   * @param timestamp timestamp for modification
   */
  public HLogEdit(Text column, byte [] bval, long timestamp) {
    this.column.set(column);
    this.val = bval;
    this.timestamp = timestamp;
  }

  /** @return the column */
  public Text getColumn() {
    return this.column;
  }

  /** @return the value */
  public byte [] getVal() {
    return this.val;
  }

  /** @return the timestamp */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * @return First column name, timestamp, and first 128 bytes of the value
   * bytes as a String.
   */
  @Override
  public String toString() {
    String value = "";
    try {
      value = (this.val.length > MAX_VALUE_LEN)?
        new String(this.val, 0, MAX_VALUE_LEN, HConstants.UTF8_ENCODING) +
          "...":
        new String(getVal(), HConstants.UTF8_ENCODING);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("UTF8 encoding not present?", e);
    }
    return "(" + getColumn().toString() + "/" + getTimestamp() + "/" +
      value + ")";
  }
  
  // Writable

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    this.column.write(out);
    out.writeInt(this.val.length);
    out.write(this.val);
    out.writeLong(timestamp);
  }
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.column.readFields(in);
    this.val = new byte[in.readInt()];
    in.readFully(this.val);
    this.timestamp = in.readLong();
  }
}
