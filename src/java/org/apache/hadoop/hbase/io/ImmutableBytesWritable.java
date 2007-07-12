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

package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** 
 * A byte sequence that is usable as a key or value.  Based on
 * {@link org.apache.hadoop.io.BytesWritable} only this class is NOT resizable
 * and DOES NOT distinguish between the size of the seqeunce and the current
 * capacity as {@link org.apache.hadoop.io.BytesWritable} does. Hence its
 * comparatively 'immutable'.
 */
public class ImmutableBytesWritable implements WritableComparable {
  private byte[] bytes;
  
  /**
   * Create a zero-size sequence.
   */
  public ImmutableBytesWritable() {
    super();
  }
  
  /**
   * Create a ImmutableBytesWritable using the byte array as the initial value.
   * @param bytes This array becomes the backing storage for the object.
   */
  public ImmutableBytesWritable(byte[] bytes) {
    this.bytes = bytes;
  }
  
  /**
   * Set the new ImmutableBytesWritable to a copy of the contents of the passed
   * <code>ibw</code>.
   * @param ibw the value to set this ImmutableBytesWritable to.
   */
  public ImmutableBytesWritable(final ImmutableBytesWritable ibw) {
    this(ibw.get(), 0, ibw.getSize());
  }
  
  /**
   * Set the value to a copy of the given byte range
   * @param newData the new values to copy in
   * @param offset the offset in newData to start at
   * @param length the number of bytes to copy
   */
  public ImmutableBytesWritable(final byte[] newData, final int offset,
      final int length) {
    this.bytes = new byte[length];
    System.arraycopy(newData, offset, this.bytes, 0, length);
  }
  
  /**
   * Get the data from the BytesWritable.
   * @return The data is only valid between 0 and getSize() - 1.
   */
  public byte[] get() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.bytes;
  }
  
  /**
   * Get the current size of the buffer.
   */
  public int getSize() {
    if (this.bytes == null) {
      throw new IllegalStateException("Uninitialiized. Null constructor " +
        "called w/o accompaying readFields invocation");
    }
    return this.bytes.length;
  }


  // inherit javadoc
  public void readFields(final DataInput in) throws IOException {
    this.bytes = new byte[in.readInt()];
    in.readFully(this.bytes, 0, this.bytes.length);
  }
  
  // inherit javadoc
  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.bytes.length);
    out.write(this.bytes, 0, this.bytes.length);
  }
  
  // Below methods copied from BytesWritable
  
  public int hashCode() {
    return WritableComparator.hashBytes(bytes, this.bytes.length);
  }
  
  /**
   * Define the sort order of the BytesWritable.
   * @param right_obj The other bytes writable
   * @return Positive if left is bigger than right, 0 if they are equal, and
   *         negative if left is smaller than right.
   */
  public int compareTo(Object right_obj) {
    return compareTo(((ImmutableBytesWritable)right_obj).get());
  }
  
  public int compareTo(final byte [] that) {
    int diff = this.bytes.length - that.length;
    return (diff != 0)?
      diff:
      WritableComparator.compareBytes(this.bytes, 0, this.bytes.length, that,
        0, that.length);
  }
  
  /**
   * Are the two byte sequences equal?
   */
  public boolean equals(Object right_obj) {
    if (right_obj instanceof ImmutableBytesWritable) {
      return compareTo(right_obj) == 0;
    }
    return false;
  }
  
  /**
   * Generate the stream of bytes as hex pairs separated by ' '.
   */
  public String toString() { 
    StringBuffer sb = new StringBuffer(3*this.bytes.length);
    for (int idx = 0; idx < this.bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  /** A Comparator optimized for ImmutableBytesWritable.
   */ 
  public static class Comparator extends WritableComparator {
    private BytesWritable.Comparator comparator =
      new BytesWritable.Comparator();
    
    public Comparator() {
      super(ImmutableBytesWritable.class);
    }
    
    /**
     * Compare the buffers in serialized form.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return comparator.compare(b1, s1, l1, b2, s2, l2);
    }
  }
  
  static { // register this comparator
    WritableComparator.define(ImmutableBytesWritable.class, new Comparator());
  }
  
  /**
   * @param array List of byte [].
   * @return Array of byte [].
   */
  public static byte [][] toArray(final List<byte []> array) {
    // List#toArray doesn't work on lists of byte [].
    byte[][] results = new byte[array.size()][];
    for (int i = 0; i < array.size(); i++) {
      results[i] = array.get(i);
    }
    return results;
  }
}