/**
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

import junit.framework.TestCase;

/**
 * Tests toString methods.
 */
public class TestToString extends TestCase {
  /**
   * tests toString methods on HSeverAddress, HServerInfo
   * @throws Exception
   */
  public void testServerInfo() throws Exception {
    final String hostport = "127.0.0.1:9999";
    HServerAddress address = new HServerAddress(hostport);
    assertEquals("HServerAddress toString", address.toString(), hostport);
    HServerInfo info = new HServerInfo(address, -1, 60030);
    assertEquals("HServerInfo", "address: " + hostport + ", startcode: -1" +
        ", load: (requests: 0 regions: 0)", info.toString());
  }

  /**
   * Test HTableDescriptor.toString();
   */
  public void testHTableDescriptor() {
    HTableDescriptor htd = HTableDescriptor.rootTableDesc;
    System. out.println(htd.toString());
    assertEquals("Table descriptor", "name: -ROOT-, families: {info:={name: " +
        "info, max versions: 1, compression: NONE, in memory: false, max " +
        "length: 2147483647, bloom filter: none}}", htd.toString());
  }
  
  /**
   * Tests HRegionInfo.toString()
   */
  public void testHRegionInfo() {
    HRegionInfo hri = HRegionInfo.rootRegionInfo;
    System.out.println(hri.toString());
    assertEquals("HRegionInfo", 
      "regionname: -ROOT-,,0, startKey: <>, tableDesc: {name: -ROOT-, " +
      "families: {info:={name: info, max versions: 1, compression: NONE, " +
      "in memory: false, max length: 2147483647, bloom filter: none}}}",
      hri.toString());
  }
}
