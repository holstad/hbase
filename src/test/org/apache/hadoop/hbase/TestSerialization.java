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

import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.Family;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
//import org.apache.hadoop.hbase.io.GetFamily;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
//import org.apache.hadoop.hbase.io.PutFamily;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.io.RowUpdates;
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

  /**
   * Test RegionInfo serialization
   * @throws Exception
   */
  public void testRowResult() throws Exception {
    HbaseMapWritable<byte [], Cell> m = new HbaseMapWritable<byte [], Cell>();
    byte [] b = Bytes.toBytes(getName());
    m.put(b, new Cell(b, System.currentTimeMillis()));
    RowResult rr = new RowResult(b, m);
    byte [] mb = Writables.getBytes(rr);
    RowResult deserializedRr =
      (RowResult)Writables.getWritable(mb, new RowResult());
    assertTrue(Bytes.equals(rr.getRow(), deserializedRr.getRow()));
    byte [] one = rr.get(b).getValue();
    byte [] two = deserializedRr.get(b).getValue();
    assertTrue(Bytes.equals(one, two));
    Writables.copyWritable(rr, deserializedRr);
    one = rr.get(b).getValue();
    two = deserializedRr.get(b).getValue();
    assertTrue(Bytes.equals(one, two));
    
  }

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
  
  /**
   * Test BatchUpdate serialization
   * @throws Exception
   */
  public void testBatchUpdate() throws Exception {
    // Add row named 'testName'.
    BatchUpdate bu = new BatchUpdate(getName());
    // Add a column named same as row.
    bu.put(getName(), getName().getBytes());
    byte [] b = Writables.getBytes(bu);
    BatchUpdate bubu =
      (BatchUpdate)Writables.getWritable(b, new BatchUpdate());
    // Assert rows are same.
    assertTrue(Bytes.equals(bu.getRow(), bubu.getRow()));
    // Assert has same number of BatchOperations.
    int firstCount = 0;
    for (BatchOperation bo: bubu) {
      firstCount++;
    }
    // Now deserialize again into same instance to ensure we're not
    // accumulating BatchOperations on each deserialization.
    BatchUpdate bububu = (BatchUpdate)Writables.getWritable(b, bubu);
    // Assert rows are same again.
    assertTrue(Bytes.equals(bu.getRow(), bububu.getRow()));
    int secondCount = 0;
    for (BatchOperation bo: bububu) {
      secondCount++;
    }
    assertEquals(firstCount, secondCount);
  }
  
 
  public void testFamily() throws Exception {
    byte [] famName = getName().getBytes();
    List<byte[]> cols = new ArrayList<byte[]>();
    cols.add((getName()+"1").getBytes());
    cols.add((getName()+"2").getBytes());
    cols.add((getName()+"3").getBytes());
    Family fam = new Family(famName, cols);
    
    byte [] mb = Writables.getBytes(fam);
    Family deserializedFam =
      (Family)Writables.getWritable(mb, new Family());
    assertTrue(Bytes.equals(fam.getFamily(), deserializedFam.getFamily()));
    cols = fam.getColumns();
    List<byte[]> deserializedCols = deserializedFam.getColumns();
    assertEquals(cols.size(), deserializedCols.size());

    for(int i=0; i<cols.size(); i++){
      assertTrue(Bytes.equals(cols.get(i), deserializedCols.get(i)));
    }
  }
  
  public void testTimeRange(String[] args) throws Exception{
    TimeRange tr = new TimeRange(5,0);
    byte [] mb = Writables.getBytes(tr);
    TimeRange deserializedTr =
      (TimeRange)Writables.getWritable(mb, new TimeRange());
    assertTrue(Bytes.equals(tr.getMax(), deserializedTr.getMax()));
    assertTrue(Bytes.equals(tr.getMin(), deserializedTr.getMin()));
  }
  
  public void testKeyValue() throws Exception {
    byte [] row = getName().getBytes();
    byte [] col = (getName()+":col").getBytes();
    KeyValue kv = new KeyValue(row, col);
    
    byte [] mb = Writables.getBytes(kv);
    KeyValue deserializedKv =
      (KeyValue)Writables.getWritable(mb, new KeyValue(row, col));
    assertEquals(kv.getLength(), deserializedKv.getLength());
    assertTrue(Bytes.equals(kv.getBuffer(), deserializedKv.getBuffer()));
  }
  
  
  public void testGet() throws Exception{
    byte [] row = (getName()+"row").getBytes();
    byte [] fam = (getName()+"fam").getBytes();
    byte [] col = (getName()+"col").getBytes();
    byte versions = (byte)1;
    TimeRange tr = new TimeRange();
    
    Get get = new GetColumns(row, fam, col, versions, tr);
    byte [] mb = Writables.getBytes(get);
    GetColumns deserializedGet =
      (GetColumns)Writables.getWritable(mb,
          new GetColumns(row, fam, col, versions, tr));
      //row
      assertTrue(Bytes.equals(get.getRow(), deserializedGet.getRow()));
      
      //families
      List<Family> fams = get.getFamilies();
      List<Family> deserializedFams = deserializedGet.getFamilies();
      assertEquals(fams.size(), deserializedFams.size());
      for(int i=0; i<fams.size(); i++){
        assertTrue(Bytes.equals(fams.get(i).getFamily(),
            deserializedFams.get(i).getFamily()));
        List<byte[]> cols = fams.get(i).getColumns();
        List<byte[]> deserializedCols = deserializedFams.get(i).getColumns();
        assertEquals(cols.size(), deserializedCols.size());

        for(int j=0; j<cols.size(); j++){
          assertTrue(Bytes.equals(cols.get(j), deserializedCols.get(j)));
        }
      }

      //versions
      assertEquals(get.getVersions(), deserializedGet.getVersions());
      
      //timeRange
      tr = get.getTimeRange();
      TimeRange deserializedTr = get.getTimeRange();
      assertTrue(Bytes.equals(tr.getMax(), deserializedTr.getMax()));
      assertTrue(Bytes.equals(tr.getMin(), deserializedTr.getMin()));
  }
  
  public void testRowUpdates() throws Exception{
    byte [] row = (getName()+"row").getBytes();
    byte [] fam = (getName()+"fam").getBytes();
    byte [] col = (getName()+"col").getBytes();
    RowUpdates rups = new RowUpdates(row, fam, col);
    
    byte[] mb = Writables.getBytes(rups);
    RowUpdates deserializedRups =
      (RowUpdates)Writables.getWritable(mb, new RowUpdates(row, fam, col));
    
    assertTrue(Bytes.equals(rups.getRow(), deserializedRups.getRow()));
    List<Family> rupsFams = rups.getFamilies();
    List<Family> deserializedRupsFams = deserializedRups.getFamilies();
    assertEquals(rupsFams.size(), deserializedRupsFams.size());
    for(int i=0; i<rupsFams.size(); i++){
      assertTrue(Bytes.equals(rupsFams.get(i).getFamily(), 
          deserializedRupsFams.get(i).getFamily()));
      List<byte[]> rupsCols = rupsFams.get(i).getColumns();
      List<byte[]> deserializedRupsCols =
        deserializedRupsFams.get(i).getColumns();
      assertEquals(rupsCols.size(), deserializedRupsCols.size());
      for(int j=0; j<rupsCols.size(); j++){
        assertTrue(Bytes.equals(rupsCols.get(j), deserializedRupsCols.get(j)));
      }
    }
    assertEquals(rups.getRowLock(), deserializedRups.getRowLock());
  }
  
}