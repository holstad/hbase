//package org.apache.hadoop.hbase.regionserver;
//
//import junit.framework.TestCase;
//
//public class TestNewGet extends TestCase {
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
  package org.apache.hadoop.hbase.regionserver;

  import java.io.IOException;
  import java.util.ArrayList;
  import java.util.List;
  import java.util.Map;
  import java.util.NavigableSet;
  import java.util.TreeSet;

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
  import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

  /**
   * {@link TestGet} is a medley of tests of get all done up as a single test.
   * This class 
   */
  public class TestNewGet extends HBaseTestCase implements HConstants {
//    private MiniDFSCluster miniHdfs;
//    
    private static byte [] row = "row1".getBytes();
    private static byte [] fam = "fam1:".getBytes();
    private static byte [] getFam = "fam1".getBytes();
    private static byte [] col1 = "col1".getBytes();
    private static byte [] col2 = "col2".getBytes();
    private static byte [] val = "val1".getBytes();
    private static byte [] column1 = Bytes.add(fam, col1);
    private static byte [] column2 = Bytes.add(fam, col2);
    @Override
    protected void setUp() throws Exception {
      super.setUp();
//      this.miniHdfs = new MiniDFSCluster(this.conf, 1, true, null);
//      // Set the hbase.rootdir to be the home directory in mini dfs.
//      this.conf.set(HConstants.HBASE_DIR,
//        this.miniHdfs.getFileSystem().getHomeDirectory().toString());
    }
    
    
    //This test does not include any test of the actual client call and keeping 
    //old way of inserting things into HBase
    public void testGetMultiHfile() throws IOException {
      HRegion region = null;
      BatchUpdate batchUpdate = null;
//      Map<byte [], Cell> results = null;
      List<KeyValue> results = new ArrayList<KeyValue>();
      KeyValue.KVComparator comp = new KeyValue.KVComparator();
      
      try {
        HTableDescriptor htd = new HTableDescriptor(getName());
        htd.addFamily(new HColumnDescriptor(fam));
//        FileSystem filesystem = FileSystem.get(conf);
//        Path rootdir = filesystem.makeQualified(
//            new Path(conf.get(HConstants.HBASE_DIR)));
//        filesystem.mkdirs(rootdir);
        
        region = createNewHRegion(htd, null, null);
       
        // write some data
        KeyValue oldKv = new KeyValue(row, column1, val);
        batchUpdate = new BatchUpdate(row);
        batchUpdate.put(column1, val);
        batchUpdate.put(column2, val);
        region.batchUpdate(batchUpdate, null);

        // flush
//        region.flushcache();

        //Crete Get object
        Get get = new GetColumns(row, getFam, col1, (byte)1, null);
        System.out.println("get " +get);
        
        // assert that getFull gives us the older value
        results = region.newget(get, results, null);
//        results = region.new getFull(row, (NavigableSet<byte []>)null, LATEST_TIMESTAMP, 1, null);
        System.out.println("got result");
        if(results.size() > 0){
//          assertEquals("olderValue", new String(results.get(0)));
          assertEquals(0, comp.compare(oldKv, results.get(0)));
        }
//        // write a new value for the cell
//        batchUpdate = new BatchUpdate(row);
//        batchUpdate.put(COLUMNS[0], "newerValue".getBytes());
//        region.batchUpdate(batchUpdate, null);
//
//        // flush
//        region.flushcache();
//        
//        // assert that getFull gives us the later value
//        results = region.getFull(row, (NavigableSet<byte []>)null, LATEST_TIMESTAMP, 1, null);
//        assertEquals("newerValue", new String(results.get(COLUMNS[0]).getValue()));
//       
//        //
//        // Test the delete masking issue
//        //
//        byte [] row2 = Bytes.toBytes("row2");
//        byte [] cell1 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "a");
//        byte [] cell2 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "b");
//        byte [] cell3 = Bytes.toBytes(Bytes.toString(COLUMNS[0]) + "c");
//        
//        // write some data at two columns
//        batchUpdate = new BatchUpdate(row2);
//        batchUpdate.put(cell1, "column0 value".getBytes());
//        batchUpdate.put(cell2, "column1 value".getBytes());
//        region.batchUpdate(batchUpdate, null);
//        
//        // flush
//        region.flushcache();
//        
//        // assert i get both columns
//        results = region.getFull(row2, (NavigableSet<byte []>)null, LATEST_TIMESTAMP, 1, null);
//        assertEquals("Should have two columns in the results map", 2, results.size());
//        assertEquals("column0 value", new String(results.get(cell1).getValue()));
//        assertEquals("column1 value", new String(results.get(cell2).getValue()));
//        
//        // write a delete for the first column
//        batchUpdate = new BatchUpdate(row2);
//        batchUpdate.delete(cell1);
//        batchUpdate.put(cell2, "column1 new value".getBytes());      
//        region.batchUpdate(batchUpdate, null);
//              
//        // flush
//        region.flushcache(); 
//        
//        // assert i get the second column only
//        results = region.getFull(row2, (NavigableSet<byte []>)null, LATEST_TIMESTAMP, 1, null);
//        System.out.println(Bytes.toString(results.keySet().iterator().next()));
//        assertEquals("Should have one column in the results map", 1, results.size());
//        assertNull("column0 value", results.get(cell1));
//        assertEquals("column1 new value", new String(results.get(cell2).getValue()));
//        
//        //
//        // Include a delete and value from the memcache in the mix
//        //
//        batchUpdate = new BatchUpdate(row2);
//        batchUpdate.delete(cell2);
//        batchUpdate.put(cell3, "column3 value!".getBytes());
//        region.batchUpdate(batchUpdate, null);
//        
//        // assert i get the third column only
//        results = region.getFull(row2, (NavigableSet<byte []>)null, LATEST_TIMESTAMP, 1, null);
//        assertEquals("Should have one column in the results map", 1, results.size());
//        assertNull("column0 value", results.get(cell1));
//        assertNull("column1 value", results.get(cell2));
//        assertEquals("column3 value!", new String(results.get(cell3).getValue()));
        
      } finally {
        if (region != null) {
          try {
            region.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
          region.getLog().closeAndDelete();
        }
      }  
    }
}
