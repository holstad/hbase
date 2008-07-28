/**
 * Copyright 2008 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Read-only table descriptor.
 */
public class UnmodifyableHTableDescriptor extends HTableDescriptor {
  public UnmodifyableHTableDescriptor() {
	  super();
  }

  /*
   * Create an unmodifyable copy of an HTableDescriptor
   * @param desc
   */
  UnmodifyableHTableDescriptor(final HTableDescriptor desc) {
    super(desc.getName(), getUnmodifyableFamilies(desc));
  }
  
  /*
   * @param desc
   * @return Families as unmodifiable array.
   */
  private static HColumnDescriptor[] getUnmodifyableFamilies(
      final HTableDescriptor desc) {
    HColumnDescriptor [] f = new HColumnDescriptor[desc.getFamilies().size()];
    int i = 0;
    for (HColumnDescriptor c: desc.getFamilies()) {
      f[i++] = c;
    }
    return f;
  }

  /**
   * Does NOT add a column family. This object is immutable
   * @param family HColumnDescriptor of familyto add.
   */
  @Override
  public void addFamily(final HColumnDescriptor family) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  /**
   * @param column
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  @Override
  public HColumnDescriptor removeFamily(final byte [] column) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setInMemory(boolean inMemory) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setValue(byte[] key, byte[] value) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setValue(String key, String value) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setMaxFileSize(long maxFileSize) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }

  @Override
  public void setMemcacheFlushSize(int memcacheFlushSize) {
    throw new UnsupportedOperationException("HTableDescriptor is read-only");
  }
}
