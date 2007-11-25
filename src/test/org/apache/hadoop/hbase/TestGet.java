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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.util.Writables;

/** Test case for get */
public class TestGet extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestGet.class.getName());
  
  private static final Text CONTENTS = new Text("contents:");
  private static final Text ROW_KEY =
    new Text(HRegionInfo.rootRegionInfo.getRegionName());
  private static final String SERVER_ADDRESS = "foo.bar.com:1234";

  
  private void verifyGet(final HRegionIncommon r, final String expectedServer)
  throws IOException {
    // This should return a value because there is only one family member
    byte [] value = r.get(ROW_KEY, CONTENTS);
    assertNotNull(value);
    
    // This should not return a value because there are multiple family members
    value = r.get(ROW_KEY, HConstants.COLUMN_FAMILY);
    assertNull(value);
    
    // Find out what getFull returns
    Map<Text, byte []> values = r.getFull(ROW_KEY);
    
    // assertEquals(4, values.keySet().size());
    for(Iterator<Text> i = values.keySet().iterator(); i.hasNext(); ) {
      Text column = i.next();
      if (column.equals(HConstants.COL_SERVER)) {
        String server = Writables.bytesToString(values.get(column));
        assertEquals(expectedServer, server);
        LOG.info(server);
      }
    }
  }
  
  /** 
   * the test
   * @throws IOException
   */
  public void testGet() throws IOException {
    MiniDFSCluster cluster = null;

    try {
      
      // Initialization
      
      cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/hbase");
      fs.mkdirs(dir);
      
      HTableDescriptor desc = new HTableDescriptor("test");
      desc.addFamily(new HColumnDescriptor(CONTENTS.toString()));
      desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY.toString()));
      
      HRegionInfo info = new HRegionInfo(desc, null, null);
      Path regionDir = HRegion.getRegionDir(dir,
          HRegionInfo.encodeRegionName(info.getRegionName()));
      fs.mkdirs(regionDir);
      
      HLog log = new HLog(fs, new Path(regionDir, "log"), conf, null);

      HRegion region = new HRegion(dir, log, fs, conf, info, null, null);
      HRegionIncommon r = new HRegionIncommon(region);
      
      // Write information to the table
      
      long lockid = r.startUpdate(ROW_KEY);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(bytes);
      CONTENTS.write(s);
      r.put(lockid, CONTENTS, bytes.toByteArray());

      bytes.reset();
      HRegionInfo.rootRegionInfo.write(s);
      
      r.put(lockid, HConstants.COL_REGIONINFO, 
          Writables.getBytes(HRegionInfo.rootRegionInfo));
      
      r.commit(lockid, System.currentTimeMillis());
      
      lockid = r.startUpdate(ROW_KEY);

      r.put(lockid, HConstants.COL_SERVER, 
        Writables.stringToBytes(new HServerAddress(SERVER_ADDRESS).toString()));
      
      r.put(lockid, HConstants.COL_STARTCODE, Writables.longToBytes(lockid));
      
      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "region"), 
        "region".getBytes(HConstants.UTF8_ENCODING));

      r.commit(lockid, System.currentTimeMillis());
      
      // Verify that get works the same from memcache as when reading from disk
      // NOTE dumpRegion won't work here because it only reads from disk.
      
      verifyGet(r, SERVER_ADDRESS);
      
      // Close and re-open region, forcing updates to disk
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, info, null, null);
      r = new HRegionIncommon(region);
      
      // Read it back
      
      verifyGet(r, SERVER_ADDRESS);
      
      // Update one family member and add a new one
      
      lockid = r.startUpdate(ROW_KEY);

      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "region"),
        "region2".getBytes(HConstants.UTF8_ENCODING));

      String otherServerName = "bar.foo.com:4321";
      r.put(lockid, HConstants.COL_SERVER, 
        Writables.stringToBytes(new HServerAddress(otherServerName).toString()));
      
      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "junk"),
        "junk".getBytes(HConstants.UTF8_ENCODING));
      
      r.commit(lockid, System.currentTimeMillis());

      verifyGet(r, otherServerName);
      
      // Close region and re-open it
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, info, null, null);
      r = new HRegionIncommon(region);

      // Read it back
      
      verifyGet(r, otherServerName);

      // Close region once and for all
      
      region.close();
      log.closeAndDelete();
      
    } finally {
      StaticTestEnvironment.shutdownDfs(cluster);
    }
  }
}
