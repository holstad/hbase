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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Test batch updates
 */
public class TestBatchUpdate extends HBaseClusterTestCase {
  private static final String CONTENTS_STR = "contents:";
  private static final byte [] CONTENTS = Bytes.toBytes(CONTENTS_STR);
  private static final String SMALLFAM_STR = "smallfam:";
  private static final byte [] SMALLFAM = Bytes.toBytes(SMALLFAM_STR);
  private static final int SMALL_LENGTH = 1;
  private static final int NB_BATCH_ROWS = 10;
  private byte[] value;
  private byte[] smallValue;

  private HTableDescriptor desc = null;
  private HTable table = null;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestBatchUpdate() throws UnsupportedEncodingException {
    super();
    value = Bytes.toBytes("abcd");
    smallValue = Bytes.toBytes("a");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(CONTENTS_STR));
    desc.addFamily(new HColumnDescriptor(SMALLFAM, 
        HColumnDescriptor.DEFAULT_VERSIONS, 
        HColumnDescriptor.DEFAULT_COMPRESSION,  
        HColumnDescriptor.DEFAULT_IN_MEMORY, 
        HColumnDescriptor.DEFAULT_BLOCKCACHE, SMALL_LENGTH, 
        HColumnDescriptor.DEFAULT_TTL, HColumnDescriptor.DEFAULT_BLOOMFILTER));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }

  /**
   * @throws IOException
   */
  public void testBatchUpdate() throws IOException {
    BatchUpdate bu = new BatchUpdate("row1");
    bu.put(CONTENTS, value);
    bu.delete(CONTENTS);
    table.commit(bu);

    bu = new BatchUpdate("row2");
    bu.put(CONTENTS, value);
    byte[][] getColumns = bu.getColumns();
    assertEquals(getColumns.length, 1);
    assertTrue(Arrays.equals(getColumns[0], CONTENTS));
    assertTrue(bu.hasColumn(CONTENTS));
    assertFalse(bu.hasColumn(new byte[] {}));
    byte[] getValue = bu.get(getColumns[0]);
    assertTrue(Arrays.equals(getValue, value));
    table.commit(bu);

    byte [][] columns = { CONTENTS };
    Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
    for (RowResult r : scanner) {
      for(Map.Entry<byte [], Cell> e: r.entrySet()) {
        System.out.println(r.getRow() + ": row: " + e.getKey() + " value: " + 
            new String(e.getValue().getValue(), HConstants.UTF8_ENCODING));
      }
    }
  }
  
  public void testBatchUpdateMaxLength() {
    // Test for a single good value
    BatchUpdate batchUpdate = new BatchUpdate("row1");
    batchUpdate.put(SMALLFAM, value);
    try {
      table.commit(batchUpdate);
      fail("Value is too long, should throw exception");
    } catch (IOException e) {
      // This is expected
    }
    // Try to see if it's still inserted
    try {
      Cell cell = table.get("row1", SMALLFAM_STR);
      assertNull(cell);
    } catch (IOException e) {
      e.printStackTrace();
      fail("This is unexpected");
    }
    // Try to put a good value
    batchUpdate = new BatchUpdate("row1");
    batchUpdate.put(SMALLFAM, smallValue);
    try {
      table.commit(batchUpdate);
    } catch (IOException e) {
      fail("Value is long enough, should not throw exception");
    }
  }
  
  public void testRowsBatchUpdate() {
    ArrayList<BatchUpdate> rowsUpdate = new ArrayList<BatchUpdate>();
    for(int i = 0; i < NB_BATCH_ROWS; i++) {
      BatchUpdate batchUpdate = new BatchUpdate("row"+i);
      batchUpdate.put(CONTENTS, value);
      rowsUpdate.add(batchUpdate);
    }
    try {
      table.commit(rowsUpdate);  
    
      byte [][] columns = { CONTENTS };
      Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
      int nbRows = 0;
      for(RowResult row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  public void testRowsBatchUpdateBufferedOneFlush() {
    table.setAutoFlush(false);
    ArrayList<BatchUpdate> rowsUpdate = new ArrayList<BatchUpdate>();
    for(int i = 0; i < NB_BATCH_ROWS*10; i++) {
      BatchUpdate batchUpdate = new BatchUpdate("row"+i);
      batchUpdate.put(CONTENTS, value);
      rowsUpdate.add(batchUpdate);
    }
    try {
      table.commit(rowsUpdate);  
    
      byte [][] columns = { CONTENTS };
      Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
      int nbRows = 0;
      for(RowResult row : scanner)
        nbRows++;
      assertEquals(0, nbRows);  
      scanner.close();
      
      table.flushCommits();
      
      scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
      nbRows = 0;
      for(RowResult row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS*10, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  public void testRowsBatchUpdateBufferedManyManyFlushes() {
    table.setAutoFlush(false);
    table.setWriteBufferSize(10);
    ArrayList<BatchUpdate> rowsUpdate = new ArrayList<BatchUpdate>();
    for(int i = 0; i < NB_BATCH_ROWS*10; i++) {
      BatchUpdate batchUpdate = new BatchUpdate("row"+i);
      batchUpdate.put(CONTENTS, value);
      rowsUpdate.add(batchUpdate);
    }
    try {
      table.commit(rowsUpdate);
      
      table.flushCommits();
      
      byte [][] columns = { CONTENTS };
      Scanner scanner = table.getScanner(columns, HConstants.EMPTY_START_ROW);
      int nbRows = 0;
      for(RowResult row : scanner)
        nbRows++;
      assertEquals(NB_BATCH_ROWS*10, nbRows);
    } catch (IOException e) {
      fail("This is unexpected : " + e);
    }
  }
  
  
}
