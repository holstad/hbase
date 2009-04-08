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
package org.apache.hadoop.hbase.client.tableindexed;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.regionserver.tableindexed.IndexedRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

public class TestIndexedTable extends HBaseClusterTestCase {

  private static final Log LOG = LogFactory.getLog(TestIndexedTable.class);

  private static final String TABLE_NAME = "table1";

  private static final byte[] FAMILY = Bytes.toBytes("family:");
  private static final byte[] COL_A = Bytes.toBytes("family:a");
  private static final String INDEX_COL_A = "A";

  private static final int NUM_ROWS = 10;
  private static final int MAX_VAL = 10000;

  private IndexedTableAdmin admin;
  private IndexedTable table;
  private Random random = new Random();

  /** constructor */
  public TestIndexedTable() {
    conf
        .set(HConstants.REGION_SERVER_IMPL, IndexedRegionServer.class.getName());
    conf.setInt("hbase.master.info.port", -1);
    conf.setInt("hbase.regionserver.info.port", -1);
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();

    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(FAMILY));

    // Create a new index that does lexicographic ordering on COL_A
    IndexSpecification colAIndex = new IndexSpecification(INDEX_COL_A,
        COL_A);
    desc.addIndex(colAIndex);

    admin = new IndexedTableAdmin(conf);
    admin.createTable(desc);
    table = new IndexedTable(conf, desc.getName());
  }

  private void writeInitalRows() throws IOException {
    for (int i = 0; i < NUM_ROWS; i++) {
      BatchUpdate update = new BatchUpdate(PerformanceEvaluation.format(i));
      byte[] colA = PerformanceEvaluation.format(random.nextInt(MAX_VAL));
      update.put(COL_A, colA);
      table.commit(update);
      LOG.info("Inserted row [" + Bytes.toString(update.getRow()) + "] val: ["
          + Bytes.toString(colA) + "]");
    }
  }


  public void testInitialWrites() throws IOException {
    writeInitalRows();
    assertRowsInOrder(NUM_ROWS);
  }
  
  private void assertRowsInOrder(int numRowsExpected) throws IndexNotFoundException, IOException {
    Scanner scanner = table.getIndexedScanner(INDEX_COL_A,
        HConstants.EMPTY_START_ROW, null, null, null);
    int numRows = 0;
    byte[] lastColA = null;
    for (RowResult rowResult : scanner) {
      byte[] colA = rowResult.get(COL_A).getValue();
      LOG.info("index scan : row [" + Bytes.toString(rowResult.getRow())
          + "] value [" + Bytes.toString(colA) + "]");
      if (lastColA != null) {
        Assert.assertTrue(Bytes.compareTo(lastColA, colA) <= 0);
      }
      lastColA = colA;
      numRows++;
    }
    Assert.assertEquals(numRowsExpected, numRows);  
  }

  public void testMultipleWrites() throws IOException {
    writeInitalRows();
    writeInitalRows(); // Update the rows.
    assertRowsInOrder(NUM_ROWS);
  }
  
  public void testDelete() throws IOException {
    writeInitalRows();
    // Delete the first row;
    table.deleteAll(PerformanceEvaluation.format(0));
    
    assertRowsInOrder(NUM_ROWS - 1);    
  }

}
