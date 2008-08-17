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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;

import java.io.*;

import org.apache.hadoop.hbase.HConstants;

/**
 * A log value.
 *
 * These aren't sortable; you need to sort by the matching HLogKey.
 * The table and row are already identified in HLogKey.
 * This just indicates the column and value.
 */
public class HLogEdit implements Writable, HConstants {

  /** Value stored for a deleted item */
  public static ImmutableBytesWritable deleteBytes = null;

  /** Value written to HLog on a complete cache flush */
  public static ImmutableBytesWritable completeCacheFlush = null;

  static {
    try {
      deleteBytes =
        new ImmutableBytesWritable("HBASE::DELETEVAL".getBytes(UTF8_ENCODING));
    
      completeCacheFlush =
        new ImmutableBytesWritable("HBASE::CACHEFLUSH".getBytes(UTF8_ENCODING));
      
    } catch (UnsupportedEncodingException e) {
      assert(false);
    }
  }
  
  /**
   * @param value
   * @return True if an entry and its content is {@link #deleteBytes}.
   */
  public static boolean isDeleted(final byte [] value) {
    return (value == null)? false: deleteBytes.compareTo(value) == 0;
  }
  
  public enum TransactionalOperation {
    START, WRITE, COMMIT, ABORT
  }

  private byte [] column;
  private byte [] val;
  private long timestamp;
  private static final int MAX_VALUE_LEN = 128;
  
  private boolean isTransactionEntry;
  private Long transactionId = null;
  private TransactionalOperation operation;


  /**
   * Default constructor used by Writable
   */
  public HLogEdit() {
    super();
  }

  /**
   * Construct a fully initialized HLogEdit
   * @param c column name
   * @param bval value
   * @param timestamp timestamp for modification
   */
  public HLogEdit(byte [] c, byte [] bval, long timestamp) {
    this.column = c;
    this.val = bval;
    this.timestamp = timestamp;
    this.isTransactionEntry = false;
  }
  
  /** Construct a WRITE transaction. 
   * 
   * @param transactionId
   * @param op
   * @param timestamp
   */
  public HLogEdit(long transactionId, BatchOperation op, long timestamp) {
    this(op.getColumn(), op.getValue(), timestamp);
    // This covers delete ops too...
    this.transactionId = transactionId;
    this.operation = TransactionalOperation.WRITE;
    this.isTransactionEntry = true;
  }

  /** Construct a transactional operation (BEGIN, ABORT, or COMMIT). 
   * 
   * @param transactionId
   * @param op
   */
  public HLogEdit(long transactionId, TransactionalOperation op) {
    this.column = new byte[0];
    this.val = new byte[0];
    this.transactionId = transactionId;
    this.operation = op;
    this.isTransactionEntry = true;
  }

  /** @return the column */
  public byte [] getColumn() {
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
  
  public boolean isTransactionEntry() {
    return isTransactionEntry;
  }
  
  /**
   * Get the transactionId, or null if this is not a transactional edit.
   * 
   * @return Return the transactionId.
   */
  public Long getTransactionId() {
    return transactionId;
  }

  /**
   * Get the operation.
   * 
   * @return Return the operation.
   */
  public TransactionalOperation getOperation() {
    return operation;
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
    return "("
        + Bytes.toString(getColumn())
        + "/"
        + getTimestamp()
        + "/"
        + (isTransactionEntry ? "tran: " + transactionId + " op "
            + operation.toString() +"/": "") + value + ")";
  }
  
  // Writable

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.column);
    if (this.val == null) {
      out.writeInt(0);
    } else {
      out.writeInt(this.val.length);
      out.write(this.val);
    }
    out.writeLong(timestamp);
    out.writeBoolean(isTransactionEntry);
    if (isTransactionEntry) {
      out.writeLong(transactionId);
      out.writeUTF(operation.name());
    }
  }
  
  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    this.column = Bytes.readByteArray(in);
    this.val = new byte[in.readInt()];
    in.readFully(this.val);
    this.timestamp = in.readLong();
    isTransactionEntry = in.readBoolean();
    if (isTransactionEntry) {
      transactionId = in.readLong();
      operation = TransactionalOperation.valueOf(in.readUTF());
    }
  }
}
