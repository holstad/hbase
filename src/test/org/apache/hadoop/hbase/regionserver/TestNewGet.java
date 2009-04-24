// package org.apache.hadoop.hbase.regionserver;
//
// import junit.framework.TestCase;
//
// public class TestNewGet extends TestCase {
/**
 * Copyright 2009 The Apache Software Foundation
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.GetFamilies;
import org.apache.hadoop.hbase.io.GetTop;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * {@link TestGet} is a medley of tests of get all done up as a single test.
 * This class
 */
public class TestNewGet extends HBaseTestCase implements HConstants {
  private final boolean PRINT = true;
  // private MiniDFSCluster miniHdfs;
  //    
  private static byte[] row = "row1".getBytes();
  private static byte[] fam = "fam1:".getBytes();
  private static byte[] getFam = "fam1".getBytes();
  private static byte[] col1 = "col1".getBytes();
  private static byte[] col2 = "col2".getBytes();
  private static byte[] col3 = "col3".getBytes();
  private static byte[] col4 = "col4".getBytes();
  private static byte[] col5 = "col5".getBytes();
  private static byte[] col6 = "col6".getBytes();

  private static byte[] val = "val1".getBytes();
  private static byte[] column1 = Bytes.add(fam, col1);
  private static byte[] column2 = Bytes.add(fam, col2);
  private static byte[] column3 = Bytes.add(fam, col3);
  private static byte[] column4 = Bytes.add(fam, col4);
  private static byte[] column5 = Bytes.add(fam, col5);
  private static byte[] column6 = Bytes.add(fam, col6);
  
  HRegion region = null;
//  BatchUpdate batchUpdate = null;
  List<KeyValue> results = null;
  
  TimeRange tr = null;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    // this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
    // // Set the hbase.rootdir to be the home directory in mini dfs.
    // this.conf.set(HConstants.HBASE_DIR,
    // this.miniHdfs.getFileSystem().getHomeDirectory().toString());
    createRegion();
    tr = new TimeRange();
  }

  protected void tearDown(){
    closeRegion();
  }
  
  // This test does not include any test of the actual client call and keeping
  // old way of inserting things into HBase
  public void testGetColumns()
  throws IOException{
    results = new ArrayList<KeyValue>();

    oldPut();
    KeyValue oldKv = new KeyValue(row, column2, val);
    // Create Get object
    Get get = new GetColumns(row, getFam, col2, (short)1, tr);
    if(PRINT) System.out.println("get " + get);
    
    //TODO see why this loops forever, something with the lock in
    //updateStorefiles this.lock.writeLock().lock();
    //Testing getting from memcache
//    results = region.newget(get, results, null);
//    System.out.println("got result with size " + results.size());
//    if (results.size() > 0) {
//      KeyValue res = results.get(0);
//      int ret = Bytes.compareTo(oldKv.getBuffer(), oldKv.getOffset(), oldKv
//          .getKeyLength() - 9, res.getBuffer(), res.getOffset(), res
//          .getKeyLength() - 9);
//      assertEquals(0, ret);
//    }

    flush();
    oldPut();
    
    get = new GetColumns(row, getFam, col2, (short)2, tr);
    results = region.newget(get, results, null);
    if(PRINT) System.out.println("got result with size " + results.size());

    if (results.size() > 0) {
      KeyValue res = results.get(0);
      int ret = Bytes.compareTo(oldKv.getBuffer(), oldKv.getOffset(), oldKv
          .getKeyLength() - 9, res.getBuffer(), res.getOffset(), res
          .getKeyLength() - 9);
      assertEquals(0, ret);
    }
    
    flush();
    oldPut();
    
    // Testing getting from memcache and storeFile 2 versions + comparing timers
    // to see which one is faster
    int v2Fetch = 3;
    results = new ArrayList<KeyValue>();
    List<byte[]> columns = new ArrayList<byte[]>(2);
    columns.add(col1);
    columns.add(col3);
    columns.add(col5);
    get = new GetColumns(row, getFam, columns, (short)v2Fetch, tr);

    long start = 0L;
    long stop = 0L;
    
    start = System.nanoTime();
    results = region.newget(get, results, null);
    stop = System.nanoTime();
    if(PRINT) System.out.println("GetColumns");
    if(PRINT) System.out.println("new timer " +(stop-start));
    int newVersions = results.size();
    if (results.size() > 0) {
      KeyValue res = results.get(0);
      int ret = Bytes.compareTo(oldKv.getBuffer(), oldKv.getOffset(), oldKv
          .getKeyLength() - 9, res.getBuffer(), res.getOffset(), res
          .getKeyLength() - 9);
      assertEquals(0, ret);
    }     
    

    //Old way of getting data 
    Map<byte [], Cell> oldRes = null;
    byte [] row = get.getRow();
    NavigableSet<byte[]> cols =
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
    cols.add(column1);
    cols.add(column3);
    cols.add(column5);
    oldRes = region.getFull(row, cols, LATEST_TIMESTAMP, v2Fetch, null);
    oldRes = null;
    start = System.nanoTime();
    oldRes = region.getFull(row, cols, LATEST_TIMESTAMP, v2Fetch, null);
    stop = System.nanoTime();
    if(PRINT) System.out.println("old timer " +(stop-start));
    int oldVersions = 0;
    for(Map.Entry<byte[], Cell> entry : oldRes.entrySet()){
      oldVersions += entry.getValue().getNumValues();
    }
    assertEquals(oldVersions, newVersions);
  }
  
  
  public void testGetFamilies()
  throws IOException {
    long start = 0L;
    long stop = 0L;
    
    results = new ArrayList<KeyValue>();
    oldPut();
    flush();
    
    Get get = new GetFamilies(row, getFam, (short)1, tr);
    region.newget(get, results, null);
    
    oldPut();
    flush();
    
    results.clear();
    
    int v2Fetch = 2;
    get = new GetFamilies(row, getFam, (short)v2Fetch, tr);
    start = System.nanoTime();
    region.newget(get, results, null);
    stop = System.nanoTime();
    if(PRINT) System.out.println("GetFamilies");
    if(PRINT) System.out.println("new timer " +(stop-start));
    
    NavigableSet<byte[]> cols =
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
      cols.add(fam);
    start = System.nanoTime();
    Map<byte[],Cell> oldRes = region.getFull(row, cols, LATEST_TIMESTAMP,
        v2Fetch, null);
    stop = System.nanoTime();
    if(PRINT) System.out.println("old timer " +(stop-start));
    int oldVersions = 0;
    for(Map.Entry<byte[], Cell> entry : oldRes.entrySet()){
      oldVersions += entry.getValue().getNumValues();
    }
    assertEquals(oldVersions, results.size());
  }
  
  public void testGetTop()
  throws IOException {
    results = new ArrayList<KeyValue>();
    long start = 0L;
    long stop = 0L;
    int nrToFetch = 5;

    
    oldPut();
    
    flush();
    oldPut();
    Get get = new GetTop(row, fam, nrToFetch, tr);
    region.newget(get, results, null);
    
    flush();
    oldPut();
    
    nrToFetch = 5;
    if(PRINT) System.out.println("Trying to fetch " +nrToFetch+
        " KeyValues");
    results = new ArrayList<KeyValue>();
    get = new GetTop(row, fam, nrToFetch, tr);
    
    start = System.nanoTime();
    region.newget(get, results, null);
    stop = System.nanoTime();
    if(PRINT) System.out.println("GetTop");
    if(PRINT) System.out.println("new timer " +(stop-start));
    if(PRINT) System.out.println("result size " +results.size());
    assertEquals(nrToFetch, results.size());
    
  }
  
  
  private void oldPut() 
  throws IOException{
    BatchUpdate batchUpdate = new BatchUpdate(row);
    batchUpdate.put(column1, val);
    batchUpdate.put(column2, val);
    batchUpdate.put(column3, val);
    batchUpdate.put(column4, val);
    batchUpdate.put(column5, val);
    region.batchUpdate(batchUpdate, null);
  }
  
  private void createRegion(){
    try {
      HTableDescriptor htd = new HTableDescriptor(getName());
      htd.addFamily(new HColumnDescriptor(fam));
      // FileSystem filesystem = FileSystem.get(conf);
      // Path rootdir = filesystem.makeQualified(
      // new Path(conf.get(HConstants.HBASE_DIR)));
      // filesystem.mkdirs(rootdir);

      region = createNewHRegion(htd, null, null);
    } catch(Exception e){}
  }
  
  private void flush()
  throws IOException{
    // flush
    region.flushcache();
  }
  
  private void closeRegion(){
    if (region != null) {
      try {
        region.close();
        region.getLog().closeAndDelete();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
  
}
