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

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents an interval of version timestamps.
 * <p>
 * Evaluated according to minStamp <= timestamp < maxStamp
 * or [minStamp,maxStamp) in interval notation.
 * <p>
 * Only used internally; should not be accessed directly by clients.
 */
public class TimeRange implements Writable {
  private byte [] minStamp = null;
  private byte [] maxStamp = null;
  private boolean allTime = false;

  /**
   * Default constructor.
   * Represents interval [0, Long.MAX_VALUE)
   */
  public TimeRange() {
	// Doesn't use another constructor to prevent needing to throw IOException
	this.minStamp = Bytes.toBytes(0L);
	this.maxStamp = Bytes.toBytes(Long.MAX_VALUE);
	this.allTime = true;
  }
  
  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(long minStamp) {
    this(Bytes.toBytes(minStamp));
  }
  
  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(byte [] minStamp) {
	this.minStamp = minStamp;
    this.maxStamp = Bytes.toBytes(Long.MAX_VALUE);
  }
  
  /**
   * Represents interval [minStamp, maxStamp) 
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   */
  public TimeRange(long minStamp, long maxStamp)
  throws IOException {
    this(Bytes.toBytes(minStamp), Bytes.toBytes(maxStamp));
  }

  /**
   * Represents interval [minStamp, maxStamp) 
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   */
  public TimeRange(byte [] minStamp, byte [] maxStamp)
  throws IOException {
    int ret = Bytes.compareTo(maxStamp, minStamp);
    if(ret <= -1) {
      throw new IOException("maxStamp is smallar than minStamp");
    }
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
  }
  
  public byte [] getMin() {
    return minStamp;
  }

  public byte [] getMax() {
    return maxStamp;
  }
  
  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false 
   * if not.
   * @param bytes timestamp to check
   * @param offset offset into the bytes
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(byte [] bytes, int offset) {
	if(allTime) return true;
	// check if >= minStamp
    int ret = Bytes.compareTo(minStamp, 0, Bytes.SIZEOF_LONG, bytes,
      offset, Bytes.SIZEOF_LONG);
    if(ret == 0) {
      return true;
    } else if(ret >= 1) {
      return false;
    }
    // check if < maxStamp
    ret = Bytes.compareTo(maxStamp, 0, Bytes.SIZEOF_LONG, bytes,
      offset, Bytes.SIZEOF_LONG);
    if(ret >= 1) {
      return true;
    }
    return false;
  }
  
  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false 
   * if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(long timestamp) {
	if(allTime) return true;
	// check if >= minStamp
    long min = Bytes.toLong(minStamp);
    if(timestamp == min) {
      return true;
    } else if(timestamp < min) {
      return false;
    }
    // check if < maxStamp
    long max = Bytes.toLong(maxStamp);
    if(timestamp < max) {
      return true;
    }
    return false;
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("maxStamp ");
    sb.append(Bytes.toLong(maxStamp));
    sb.append(", minStamp");
    sb.append(Bytes.toLong(minStamp));
    return sb.toString();
  }
  
  //Writable
  public void readFields(final DataInput in) throws IOException {
    this.minStamp = Bytes.readByteArray(in);
    this.maxStamp = Bytes.readByteArray(in);
    this.allTime = in.readBoolean();
  }
  
  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.minStamp);
    Bytes.writeByteArray(out, this.maxStamp);
    out.writeBoolean(this.allTime);
  }
}
