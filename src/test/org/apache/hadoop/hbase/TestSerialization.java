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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.filter.InclusiveStopRowFilter;
import org.apache.hadoop.hbase.io.Delete;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.Put;
import org.apache.hadoop.hbase.io.Scan;
import org.apache.hadoop.hbase.io.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Test HBase Writables serializations
 */
public class TestSerialization extends HBaseTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testHbaseMapWritable() throws Exception {
    HbaseMapWritable<byte [], byte []> hmw =
      new HbaseMapWritable<byte[], byte[]>();
    hmw.put("key".getBytes(), "value".getBytes());
    byte [] bytes = Writables.getBytes(hmw);
    hmw = (HbaseMapWritable<byte[], byte[]>)
      Writables.getWritable(bytes, new HbaseMapWritable<byte [], byte []>());
    assertTrue(hmw.size() == 1);
    assertTrue(Bytes.equals("value".getBytes(), hmw.get("key".getBytes())));
  }
  
  public void testHMsg() throws Exception {
    HMsg  m = new HMsg(HMsg.Type.MSG_REGIONSERVER_QUIESCE);
    byte [] mb = Writables.getBytes(m);
    HMsg deserializedHMsg = (HMsg)Writables.getWritable(mb, new HMsg());
    assertTrue(m.equals(deserializedHMsg));
    m = new HMsg(HMsg.Type.MSG_REGIONSERVER_QUIESCE,
      new HRegionInfo(new HTableDescriptor(getName()),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY),
        "Some message".getBytes());
    mb = Writables.getBytes(m);
    deserializedHMsg = (HMsg)Writables.getWritable(mb, new HMsg());
    assertTrue(m.equals(deserializedHMsg));
  }
  
  public void testTableDescriptor() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName());
    byte [] mb = Writables.getBytes(htd);
    HTableDescriptor deserializedHtd =
      (HTableDescriptor)Writables.getWritable(mb, new HTableDescriptor());
    assertEquals(htd.getNameAsString(), deserializedHtd.getNameAsString());
  }

//  /**
//   * Test RegionInfo serialization
//   * @throws Exception
//   */
//  public void testRowResult() throws Exception {
//    HbaseMapWritable<byte [], Cell> m = new HbaseMapWritable<byte [], Cell>();
//    byte [] b = Bytes.toBytes(getName());
//    m.put(b, new Cell(b, System.currentTimeMillis()));
//    RowResult rr = new RowResult(b, m);
//    byte [] mb = Writables.getBytes(rr);
//    RowResult deserializedRr =
//      (RowResult)Writables.getWritable(mb, new RowResult());
//    assertTrue(Bytes.equals(rr.getRow(), deserializedRr.getRow()));
//    byte [] one = rr.get(b).getValue();
//    byte [] two = deserializedRr.get(b).getValue();
//    assertTrue(Bytes.equals(one, two));
//    Writables.copyWritable(rr, deserializedRr);
//    one = rr.get(b).getValue();
//    two = deserializedRr.get(b).getValue();
//    assertTrue(Bytes.equals(one, two));
//    
//  }

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  public void testRegionInfo() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(getName());
    String [] families = new String [] {"info:", "anchor:"};
    for (int i = 0; i < families.length; i++) {
      htd.addFamily(new HColumnDescriptor(families[i]));
    }
    HRegionInfo hri = new HRegionInfo(htd,
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    byte [] hrib = Writables.getBytes(hri);
    HRegionInfo deserializedHri =
      (HRegionInfo)Writables.getWritable(hrib, new HRegionInfo());
    assertEquals(hri.getEncodedName(), deserializedHri.getEncodedName());
    assertEquals(hri.getTableDesc().getFamilies().size(),
      deserializedHri.getTableDesc().getFamilies().size());
  }
  
  /**
   * Test ServerInfo serialization
   * @throws Exception
   */
  public void testServerInfo() throws Exception {
    HServerInfo hsi = new HServerInfo(new HServerAddress("0.0.0.0:123"), -1,
      1245);
    byte [] b = Writables.getBytes(hsi);
    HServerInfo deserializedHsi =
      (HServerInfo)Writables.getWritable(b, new HServerInfo());
    assertTrue(hsi.equals(deserializedHsi));
  }
  
//  /**
//   * Test BatchUpdate serialization
//   * @throws Exception
//   */
//  public void testBatchUpdate() throws Exception {
//    // Add row named 'testName'.
//    BatchUpdate bu = new BatchUpdate(getName());
//    // Add a column named same as row.
//    bu.put(getName(), getName().getBytes());
//    byte [] b = Writables.getBytes(bu);
//    BatchUpdate bubu =
//      (BatchUpdate)Writables.getWritable(b, new BatchUpdate());
//    // Assert rows are same.
//    assertTrue(Bytes.equals(bu.getRow(), bubu.getRow()));
//    // Assert has same number of BatchOperations.
//    int firstCount = 0;
//    for (BatchOperation bo: bubu) {
//      firstCount++;
//    }
//    // Now deserialize again into same instance to ensure we're not
//    // accumulating BatchOperations on each deserialization.
//    BatchUpdate bububu = (BatchUpdate)Writables.getWritable(b, bubu);
//    // Assert rows are same again.
//    assertTrue(Bytes.equals(bu.getRow(), bububu.getRow()));
//    int secondCount = 0;
//    for (BatchOperation bo: bububu) {
//      secondCount++;
//    }
//    assertEquals(firstCount, secondCount);
//  }
  
 
  public void testPut() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    byte[] qf2 = "qf2".getBytes();
    byte[] qf3 = "qf3".getBytes();
    byte[] qf4 = "qf4".getBytes();
    byte[] qf5 = "qf5".getBytes();
    byte[] qf6 = "qf6".getBytes();
    byte[] qf7 = "qf7".getBytes();
    byte[] qf8 = "qf8".getBytes();
    
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();
    
    Put put = new Put(row);
    KeyValue added = put.add(fam, qf1, ts, val);
    put.add(fam, qf2, ts, val);
    put.add(fam, qf3, ts, val);
    put.add(fam, qf4, ts, val);
    put.add(fam, qf5, ts, val);
    put.add(fam, qf6, ts, val);
    put.add(fam, qf7, ts, val);
    put.add(fam, qf8, ts, val);
    
    byte[] sb = Writables.getBytes(put);
    Put desPut = (Put)Writables.getWritable(sb, new Put());

    //Timing test
//    long start = System.nanoTime();
//    desPut = (Put)Writables.getWritable(sb, new Put());
//    long stop = System.nanoTime();
//    System.out.println("timer " +(stop-start));
    
    assertTrue(Bytes.equals(put.getRow(), desPut.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry : put.getFamilyMap().entrySet()){
      assertTrue(desPut.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desPut.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }

  public void testDelete() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();
    
    Delete delete = new Delete(row);
    delete.deleteColumn(fam, qf1, ts);
    
    byte[] sb = Writables.getBytes(delete);
    Delete desDelete = (Delete)Writables.getWritable(sb, new Delete());

    assertTrue(Bytes.equals(delete.getRow(), desDelete.getRow()));
    List<KeyValue> list = null;
    List<KeyValue> desList = null;
    for(Map.Entry<byte[], List<KeyValue>> entry :
        delete.getFamilyMap().entrySet()){
      assertTrue(desDelete.getFamilyMap().containsKey(entry.getKey()));
      list = entry.getValue();
      desList = desDelete.getFamilyMap().get(entry.getKey());
      for(int i=0; i<list.size(); i++){
        assertTrue(list.get(i).equals(desList.get(i)));
      }
    }
  }
 
  public void testGet() throws Exception{
    byte[] row = "row".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();
    int maxVersions = 2;
    long rowLock = 5;
    
    Get get = new Get(row, rowLock);
    get.addColumn(fam, qf1);
    get.setTimeRange(ts, ts);
    get.setMaxVersions(maxVersions);
    
    byte[] sb = Writables.getBytes(get);
    Get desGet = (Get)Writables.getWritable(sb, new Get());

    assertTrue(Bytes.equals(get.getRow(), desGet.getRow()));
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;
    
    for(Map.Entry<byte[], Set<byte[]>> entry :
        get.getFamilyMap().entrySet()){
      assertTrue(desGet.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desGet.getFamilyMap().get(entry.getKey());
      for(byte[] column : set){
        assertTrue(desSet.contains(column));
      }
    }
    
    assertEquals(get.getRowLock(), desGet.getRowLock());
    assertEquals(get.getRowLock(), desGet.getRowLock());
    assertEquals(get.getMaxVersions(), desGet.getMaxVersions());
    TimeRange tr = get.getTimeRange();
    TimeRange desTr = desGet.getTimeRange();
    assertEquals(0, Bytes.compareTo(tr.getMax(), desTr.getMax()));
    assertEquals(0, Bytes.compareTo(tr.getMin(), desTr.getMin()));
  }
  

  public void testScan() throws Exception{
    byte[] startRow = "startRow".getBytes();
    byte[] stopRow  = "stopRow".getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf1 = "qf1".getBytes();
    
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();
    int maxVersions = 2;
    long rowLock = 5;
    
    Scan scan = new Scan(startRow, stopRow);
    scan.addColumn(fam, qf1);
    scan.setTimeRange(ts, ts);
    scan.setMaxVersions(maxVersions);
    scan.setFilter(new InclusiveStopRowFilter(stopRow));
    
    byte[] sb = Writables.getBytes(scan);
    Scan desScan = (Scan)Writables.getWritable(sb, new Scan());

    assertTrue(Bytes.equals(scan.getStartRow(), desScan.getStartRow()));
    assertTrue(Bytes.equals(scan.getStopRow(), desScan.getStopRow()));
    Set<byte[]> set = null;
    Set<byte[]> desSet = null;
    
    for(Map.Entry<byte[], Set<byte[]>> entry :
        scan.getFamilyMap().entrySet()){
      assertTrue(desScan.getFamilyMap().containsKey(entry.getKey()));
      set = entry.getValue();
      desSet = desScan.getFamilyMap().get(entry.getKey());
      for(byte[] column : set){
        assertTrue(desSet.contains(column));
      }
    }
    
    assertEquals(scan.getMaxVersions(), desScan.getMaxVersions());
    TimeRange tr = scan.getTimeRange();
    TimeRange desTr = desScan.getTimeRange();
    assertEquals(0, Bytes.compareTo(tr.getMax(), desTr.getMax()));
    assertEquals(0, Bytes.compareTo(tr.getMin(), desTr.getMin()));
    
    assertTrue(desScan.getFilter() instanceof InclusiveStopRowFilter);
  }
  
  
  
  
//  public void testFamily() throws Exception {
//    byte [] famName = getName().getBytes();
//    List<byte[]> cols = new ArrayList<byte[]>();
//    cols.add((getName()+"1").getBytes());
//    cols.add((getName()+"2").getBytes());
//    cols.add((getName()+"3").getBytes());
//    Family fam = new Family(famName, cols);
//    
//    byte [] mb = Writables.getBytes(fam);
//    Family deserializedFam =
//      (Family)Writables.getWritable(mb, new Family());
//    assertTrue(Bytes.equals(fam.getFamily(), deserializedFam.getFamily()));
//    cols = fam.getColumns();
//    List<byte[]> deserializedCols = deserializedFam.getColumns();
//    assertEquals(cols.size(), deserializedCols.size());
//
//    for(int i=0; i<cols.size(); i++){
//      assertTrue(Bytes.equals(cols.get(i), deserializedCols.get(i)));
//    }
//  }
  
  public void testTimeRange(String[] args) throws Exception{
    TimeRange tr = new TimeRange(5,0);
    byte [] mb = Writables.getBytes(tr);
    TimeRange deserializedTr =
      (TimeRange)Writables.getWritable(mb, new TimeRange());
    assertTrue(Bytes.equals(tr.getMax(), deserializedTr.getMax()));
    assertTrue(Bytes.equals(tr.getMin(), deserializedTr.getMin()));
  }
  
  public void testKeyValue() throws Exception {
    byte[] row = getName().getBytes();
    byte [] col = (getName()+":col").getBytes();
    byte[] fam = "fam".getBytes();
    byte[] qf = "qf".getBytes();
    long ts = System.currentTimeMillis();
    byte[] val = "val".getBytes();
    
    KeyValue kv = new KeyValue(row, fam, qf, ts, val);
//    KeyValue kv = new KeyValue(row, col);
    
    byte [] mb = Writables.getBytes(kv);
    KeyValue deserializedKv =
      (KeyValue)Writables.getWritable(mb, new KeyValue());
    assertTrue(Bytes.equals(kv.getBuffer(), deserializedKv.getBuffer()));
    assertEquals(kv.getOffset(), deserializedKv.getOffset());
    assertEquals(kv.getLength(), deserializedKv.getLength());
  }
  
  
//  public void testGet() throws Exception{
//    byte [] row = (getName()+"row").getBytes();
//    byte [] fam = (getName()+"fam").getBytes();
//    byte [] col = (getName()+"col").getBytes();
//    byte versions = (byte)1;
//    TimeRange tr = new TimeRange();
//    
//    Get get = new GetColumns(row, fam, col, versions, tr);
//    byte [] mb = Writables.getBytes(get);
//    GetColumns deserializedGet =
//      (GetColumns)Writables.getWritable(mb,
//          new GetColumns(row, fam, col, versions, tr));
//      //row
//      assertTrue(Bytes.equals(get.getRow(), deserializedGet.getRow()));
//      
//      //families
//      List<Family> fams = get.getFamilies();
//      List<Family> deserializedFams = deserializedGet.getFamilies();
//      assertEquals(fams.size(), deserializedFams.size());
//      for(int i=0; i<fams.size(); i++){
//        assertTrue(Bytes.equals(fams.get(i).getFamily(),
//            deserializedFams.get(i).getFamily()));
//        List<byte[]> cols = fams.get(i).getColumns();
//        List<byte[]> deserializedCols = deserializedFams.get(i).getColumns();
//        assertEquals(cols.size(), deserializedCols.size());
//
//        for(int j=0; j<cols.size(); j++){
//          assertTrue(Bytes.equals(cols.get(j), deserializedCols.get(j)));
//        }
//      }
//
//      //versions
//      assertEquals(get.getVersions(), deserializedGet.getVersions());
//      
//      //timeRange
//      tr = get.getTimeRange();
//      TimeRange deserializedTr = get.getTimeRange();
//      assertTrue(Bytes.equals(tr.getMax(), deserializedTr.getMax()));
//      assertTrue(Bytes.equals(tr.getMin(), deserializedTr.getMin()));
//  }
  
//  public void testRowUpdates() throws Exception{
//    byte [] row = (getName()+"row").getBytes();
//    byte [] fam = (getName()+"fam").getBytes();
//    byte [] col = (getName()+"col").getBytes();
//    RowUpdates rups = new RowUpdates(row, fam, col);
//    
//    byte[] mb = Writables.getBytes(rups);
//    RowUpdates deserializedRups =
//      (RowUpdates)Writables.getWritable(mb, new RowUpdates(row, fam, col));
//    
//    assertTrue(Bytes.equals(rups.getRow(), deserializedRups.getRow()));
//    List<Family> rupsFams = rups.getFamilies();
//    List<Family> deserializedRupsFams = deserializedRups.getFamilies();
//    assertEquals(rupsFams.size(), deserializedRupsFams.size());
//    for(int i=0; i<rupsFams.size(); i++){
//      assertTrue(Bytes.equals(rupsFams.get(i).getFamily(), 
//          deserializedRupsFams.get(i).getFamily()));
//      List<byte[]> rupsCols = rupsFams.get(i).getColumns();
//      List<byte[]> deserializedRupsCols =
//        deserializedRupsFams.get(i).getColumns();
//      assertEquals(rupsCols.size(), deserializedRupsCols.size());
//      for(int j=0; j<rupsCols.size(); j++){
//        assertTrue(Bytes.equals(rupsCols.get(j), deserializedRupsCols.get(j)));
//      }
//    }
//    assertEquals(rups.getRowLock(), deserializedRups.getRowLock());
//  }
  
  
  
  
}