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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 */
public class TestZooKeeper extends HBaseClusterTestCase {
  private static class EmptyWatcher implements Watcher {
    public EmptyWatcher() {}
    public void process(WatchedEvent event) {}
  }

  @Override
  protected void setUp() throws Exception {
    setOpenMetaTable(false);
    super.setUp();
  }

  /** 
   * @throws IOException
   */
  public void testWritesRootRegionLocation() throws IOException {
    ZooKeeperWrapper zooKeeper = new ZooKeeperWrapper(conf);

    boolean outOfSafeMode = zooKeeper.checkOutOfSafeMode();
    assertFalse(outOfSafeMode);

    HServerAddress zooKeeperRootAddress = zooKeeper.readRootRegionLocation();
    assertNull(zooKeeperRootAddress);

    HMaster master = cluster.getMaster();
    HServerAddress masterRootAddress = master.getRootRegionLocation();
    assertNull(masterRootAddress);

    new HTable(conf, HConstants.META_TABLE_NAME);

    outOfSafeMode = zooKeeper.checkOutOfSafeMode();
    assertTrue(outOfSafeMode);

    zooKeeperRootAddress = zooKeeper.readRootRegionLocation();
    assertNotNull(zooKeeperRootAddress);

    masterRootAddress = master.getRootRegionLocation();
    assertEquals(masterRootAddress, zooKeeperRootAddress);
  }

  /**
   * @throws IOException
   */
  public void testParentExists() throws IOException {
    conf.set("zookeeper.znode.safemode", "/a/b/c/d/e");
    ZooKeeperWrapper zooKeeper = new ZooKeeperWrapper(conf);
    assertTrue(zooKeeper.writeOutOfSafeMode());
  }

  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  public void testClientSessionExpired() throws IOException, InterruptedException {
    new HTable(conf, HConstants.META_TABLE_NAME);

    String quorumServers = zooKeeperCluster.getQuorumServers();
    int sessionTimeout = conf.getInt("zookeeper.session.timeout", 2 * 1000);
    Watcher watcher = new EmptyWatcher();
    HConnection connection = HConnectionManager.getConnection(conf);
    ZooKeeperWrapper connectionZK = connection.getZooKeeperWrapper();
    long sessionID = connectionZK.getSessionID();
    byte[] password = connectionZK.getSessionPassword();

    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout, watcher, sessionID, password);
    zk.close();

    Thread.sleep(sessionTimeout * 3);

    System.err.println("ZooKeeper should have timed out");
    connection.relocateRegion(HConstants.ROOT_TABLE_NAME, HConstants.EMPTY_BYTE_ARRAY);
  }
}
