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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.Family;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.RowUpdates;
import org.apache.hadoop.hbase.io.TimeRange;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tests HTable
 */
public class TestNewHTable extends HBaseClusterTestCase implements HConstants {
//  private static final HColumnDescriptor column =
//    new HColumnDescriptor(COLUMN_FAMILY);
  HTable table = null;
  
  private static final byte [] nosuchTable = Bytes.toBytes("nosuchTable");
  private static final byte [] tableAname = Bytes.toBytes("tableA");
  private static final byte [] tableBname = Bytes.toBytes("tableB");
  
  private static final byte [] row = Bytes.toBytes("row");
 
  private static final byte [] attrName = Bytes.toBytes("TESTATTR");
  private static final byte [] attrValue = Bytes.toBytes("somevalue");
  
  private final int SIZE = 5;
  private String familyName = "family";
//  private String family = familyName+':';
  private byte[] fam = familyName.getBytes();
  private String col = "col";
  private List<byte[]> columns = null;


  protected void setUp()
  throws Exception{
    super.setUp();
    
    HColumnDescriptor colDes =
      new HColumnDescriptor((familyName+':').getBytes());
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor testTableADesc =
      new HTableDescriptor(tableAname);
    testTableADesc.addFamily(colDes);
//    testTableADesc.addFamily(column2);
    admin.createTable(testTableADesc);
    
    table = new HTable(conf, tableAname);
    
    columns = new ArrayList<byte[]>();
    for(int i=0; i<SIZE; i++){
      columns.add((col+i).getBytes());
    }
  }
  
  
//  public void stestTest() throws IOException{
//    byte[] row = "test5:".getBytes();
//    
//    Family family = new Family();
////    Family[] families = {family};
//    
//    List<Family> families = new ArrayList<Family>();
//    families.add(family);
////    Family[] families = {new Family()};
////    TimeRange tr = new TimeRange();
////    byte[] res = table.testGet(row, families, tr);
//    byte[] column = "col".getBytes();
//    short versions = 1;
//    TimeRange tr = new TimeRange();
//    
////    byte[] family = "fam".getBytes();
////    Get get = new GetColumns(row, family, column, versions);
////    TestClass tc = new TestClass(tr);
////    TestClass tc = new TestClass(row, tr, versions);
////    TestClass tc = new TestCslass();
////    TestClass tc = new TestClass(row);
//    TestClass tc = new TestClass(row, families, versions, tr);
//    
//    byte[] res = table.testGet(row, tc);
//    System.out.println("res " +new String(res));
//    
//  }
  
  public void stestNewPut()
  throws IOException{
    RowUpdates updates = new RowUpdates(row);
    Family family = new Family(fam, columns, columns);
    updates.add(family);
    System.out.println("Commiting");
    table.newCommit(updates);
    System.out.println("done");
  }
  
  public void testNewGetColumns() {
//    HTable table = null;
//    final int SIZE = 5;
//    String familyName = "info2";
    String family = familyName+':';
//    byte[] fam = familyName.getBytes();
    
//    String col = "col";
//    List<byte[]> columns = new ArrayList<byte[]>();
//    for(int i=0; i<SIZE; i++){
//      columns.add(Bytes.toBytes(col +i));
//    }
    
    try {
//      HColumnDescriptor column =
//        new HColumnDescriptor(family.getBytes());
//      HBaseAdmin admin = new HBaseAdmin(conf);
//      HTableDescriptor testTableADesc =
//        new HTableDescriptor(tableAname);
//      testTableADesc.addFamily(column);
////      testTableADesc.addFamily(column2);
//      admin.createTable(testTableADesc);
//      
//      table = new HTable(conf, tableAname);
      BatchUpdate batchUpdate = new BatchUpdate(row);
      
      for(int i = 0; i < 5; i++)
        batchUpdate.put(family+i, Bytes.toBytes(i));
      
      table.commit(batchUpdate);
      System.out.println("updates commited");
//      byte[] row, byte[] fam, byte[] column, short versions)
      Get get = new GetColumns(row, fam, "1".getBytes(), (short)1);
      KeyValue[] result = table.get(get);
      System.out.println("result size " +result.length);
//      assertTrue(table.exists(row));
//      for(int i = 0; i < 5; i++)
//        assertTrue(table.exists(row, Bytes.toBytes(COLUMN_FAMILY_STR+i)));
//
//      RowResult result = null;
//      result = table.getRow(row,  new byte[][] {COLUMN_FAMILY});
//      for(int i = 0; i < 5; i++)
//        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));
//      
//      result = table.getRow(row);
//      for(int i = 0; i < 5; i++)
//        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));
//
//      batchUpdate = new BatchUpdate(row);
//      batchUpdate.put("info2:a", Bytes.toBytes("a"));
//      table.commit(batchUpdate);
//      
//      result = table.getRow(row, new byte[][] { COLUMN_FAMILY,
//          Bytes.toBytes("info2:a") });
//      for(int i = 0; i < 5; i++)
//        assertTrue(result.containsKey(Bytes.toBytes(COLUMN_FAMILY_STR+i)));
//      assertTrue(result.containsKey(Bytes.toBytes("info2:a")));
    } catch (IOException e) {
      e.printStackTrace();
      fail("Should not have any exception " +
        e.getClass());
    }
  }
}
