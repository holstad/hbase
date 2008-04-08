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

import java.io.IOException;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.MultiRegionTable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.Cell;

/**
 * {@Link TestHRegion} does a split but this TestCase adds testing of fast
 * split and manufactures odd-ball split scenarios.
 */
public class TestSplit extends MultiRegionTable {
  @SuppressWarnings("hiding")
  static final Log LOG = LogFactory.getLog(TestSplit.class.getName());
  
  /** constructor */
  public TestSplit() {
    super();

    // Always compact if there is more than one store file.
    conf.setInt("hbase.hstore.compactionThreshold", 2);
    
    // Make lease timeout longer, lease checks less frequent
    conf.setInt("hbase.master.lease.period", 10 * 1000);
    conf.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);

    // Increase the amount of time between client retries
    conf.setLong("hbase.client.pause", 15 * 1000);

    // This size should make it so we always split using the addContent
    // below.  After adding all data, the first region is 1.3M
    conf.setLong("hbase.hregion.max.filesize", 1024 * 128);
    
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(this.getClass().getPackage().getName()).
      setLevel(Level.DEBUG);
  }

  /**
   * Splits twice and verifies getting from each of the split regions.
   * @throws Exception
   */
  public void testBasicSplit() throws Exception {
    HRegion region = null;
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      htd.addFamily(new HColumnDescriptor(COLFAMILY_NAME3));
      region = createNewHRegion(htd, null, null);
      basicSplit(region);
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  private void basicSplit(final HRegion region) throws Exception {
    addContent(region, COLFAMILY_NAME3);
    region.flushcache();
    Text midkey = region.compactStores();
    assertNotNull(midkey);
    HRegion [] regions = split(region, midkey);
    try {
      // Need to open the regions.
      // TODO: Add an 'open' to HRegion... don't do open by constructing
      // instance.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = openClosedRegion(regions[i]);
      }
      // Assert can get rows out of new regions. Should be able to get first
      // row from first region and the midkey from second region.
      assertGet(regions[0], COLFAMILY_NAME3, new Text(START_KEY));
      assertGet(regions[1], COLFAMILY_NAME3, midkey);
      // Test I can get scanner and that it starts at right place.
      assertScan(regions[0], COLFAMILY_NAME3, new Text(START_KEY));
      assertScan(regions[1], COLFAMILY_NAME3, midkey);
      // Now prove can't split regions that have references.
      for (int i = 0; i < regions.length; i++) {
        // Add so much data to this region, we create a store file that is >
        // than one of our unsplitable references. it will.
        for (int j = 0; j < 2; j++) {
          addContent(regions[i], COLFAMILY_NAME3);
        }
        addContent(regions[i], COLFAMILY_NAME2);
        addContent(regions[i], COLFAMILY_NAME1);
        regions[i].flushcache();
      }

      Text[] midkeys = new Text[regions.length];
      // To make regions splitable force compaction.
      for (int i = 0; i < regions.length; i++) {
        midkeys[i] = regions[i].compactStores();
      }

      TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
      // Split these two daughter regions so then I'll have 4 regions. Will
      // split because added data above.
      for (int i = 0; i < regions.length; i++) {
        HRegion[] rs = null;
        if (midkeys[i] != null) {
          rs = split(regions[i], midkeys[i]);
          for (int j = 0; j < rs.length; j++) {
            sortedMap.put(rs[j].getRegionName().toString(),
              openClosedRegion(rs[j]));
          }
        }
      }
      LOG.info("Made 4 regions");
      // The splits should have been even. Test I can get some arbitrary row out
      // of each.
      int interval = (LAST_CHAR - FIRST_CHAR) / 3;
      byte[] b = START_KEY.getBytes(HConstants.UTF8_ENCODING);
      for (HRegion r : sortedMap.values()) {
        assertGet(r, COLFAMILY_NAME3, new Text(new String(b,
            HConstants.UTF8_ENCODING)));
        b[0] += interval;
      }
    } finally {
      for (int i = 0; i < regions.length; i++) {
        try {
          regions[i].close();
        } catch (IOException e) {
          // Ignore.
        }
      }
    }
  }
  
  private void assertGet(final HRegion r, final String family, final Text k)
  throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Cell[] results = r.get(k, new Text(family),
      Integer.MAX_VALUE);
    for (int j = 0; j < results.length; j++) {
      Text tmp = new Text(results[j].getValue());
      // Row should be equal to value every time.
      assertEquals(k.toString(), tmp.toString());
    }
  }
  
  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   * @param r
   * @param column
   * @param firstValue
   * @throws IOException
   */
  private void assertScan(final HRegion r, final String column,
      final Text firstValue)
  throws IOException {
    Text [] cols = new Text[] {new Text(column)};
    InternalScanner s = r.getScanner(cols,
      HConstants.EMPTY_START_ROW, System.currentTimeMillis(), null);
    try {
      HStoreKey curKey = new HStoreKey();
      TreeMap<Text, byte []> curVals = new TreeMap<Text, byte []>();
      boolean first = true;
      OUTER_LOOP: while(s.next(curKey, curVals)) {
        for(Text col: curVals.keySet()) {
          byte [] val = curVals.get(col);
          Text curval = new Text(val);
          if (first) {
            first = false;
            assertTrue(curval.compareTo(firstValue) == 0);
          } else {
            // Not asserting anything.  Might as well break.
            break OUTER_LOOP;
          }
        }
      }
    } finally {
      s.close();
    }
  }
  
  private HRegion [] split(final HRegion r, final Text midKey)
  throws IOException {
    // Assert can get mid key from passed region.
    assertGet(r, COLFAMILY_NAME3, midKey);
    HRegion [] regions = r.splitRegion(null, midKey);
    assertEquals(regions.length, 2);
    return regions;
  }
}
