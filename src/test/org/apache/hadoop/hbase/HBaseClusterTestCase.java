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

import java.io.PrintWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.Text;

/**
 * Abstract base class for HBase cluster junit tests.  Spins up an hbase
 * cluster in setup and tears it down again in tearDown.
 */
public abstract class HBaseClusterTestCase extends HBaseTestCase {
  private static final Log LOG =
    LogFactory.getLog(HBaseClusterTestCase.class.getName());
  
  protected MiniHBaseCluster cluster;
  protected MiniDFSCluster dfsCluster;
  protected int regionServers;
  protected boolean startDfs;
  
  public HBaseClusterTestCase() {
    this(1);
  }
  
  /**
   * Start a MiniHBaseCluster with regionServers region servers in-process to
   * start with. Also, start a MiniDfsCluster before starting the hbase cluster.
   * The configuration used will be edited so that this works correctly.
   */  
  public HBaseClusterTestCase(int regionServers) {
    this(regionServers, true);
  }
  
  /**
   * Start a MiniHBaseCluster with regionServers region servers in-process to
   * start with. Optionally, startDfs indicates if a MiniDFSCluster should be
   * started. If startDfs is false, the assumption is that an external DFS is
   * configured in hbase-site.xml and is already started, or you have started a
   * MiniDFSCluster on your own and edited the configuration in memory. (You 
   * can modify the config used by overriding the preHBaseClusterSetup method.)
   */
  public HBaseClusterTestCase(int regionServers, boolean startDfs) {
    super();
    this.startDfs = startDfs;
    this.regionServers = regionServers;
  }
  
  /**
   * Run after dfs is ready but before hbase cluster is started up.
   */
  protected void preHBaseClusterSetup() throws Exception {
  } 

  /**
   * Actually start the MiniHBase instance.
   */
  protected void HBaseClusterSetup() throws Exception {
    // start the mini cluster
    this.cluster = new MiniHBaseCluster(conf, regionServers);
    HTable meta = new HTable(conf, new Text(".META."));
  }
  
  /**
   * Run after hbase cluster is started up.
   */
  protected void postHBaseClusterSetup() throws Exception {
  } 

  @Override
  protected void setUp() throws Exception {
    try {
      if (startDfs) {
        // start up the dfs
        dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);

        // mangle the conf so that the fs parameter points to the minidfs we
        // just started up
        FileSystem fs = dfsCluster.getFileSystem();
        conf.set("fs.default.name", fs.getName());      
        Path parentdir = fs.getHomeDirectory();
        conf.set(HConstants.HBASE_DIR, parentdir.toString());
        fs.mkdirs(parentdir);
        FSUtils.setVersion(fs, parentdir);
      }

      // do the super setup now. if we had done it first, then we would have
      // gotten our conf all mangled and a local fs started up.
      super.setUp();
    
      // run the pre-cluster setup
      preHBaseClusterSetup();    
    
      // start the instance
      HBaseClusterSetup();
      
      // run post-cluster setup
      postHBaseClusterSetup();
    } catch (Exception e) {
      LOG.error("Exception in setup!", e);
      if (cluster != null) {
        cluster.shutdown();
      }
      if (dfsCluster != null) {
        StaticTestEnvironment.shutdownDfs(dfsCluster);
      }
      throw e;
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    try {
      HConnectionManager.deleteConnection(conf);
      if (this.cluster != null) {
        try {
          this.cluster.shutdown();
        } catch (Exception e) {
          LOG.warn("Closing mini dfs", e);
        }
      }
      if (startDfs) {
        StaticTestEnvironment.shutdownDfs(dfsCluster);
      }
    } catch (Exception e) {
      LOG.error(e);
    }
    // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
    //  "Temporary end-of-test thread dump debugging HADOOP-2040: " + getName());
  }

  
  /**
   * Use this utility method debugging why cluster won't go down.  On a
   * period it throws a thread dump.  Method ends when all cluster
   * regionservers and master threads are no long alive.
   */
  public void threadDumpingJoin() {
    if (this.cluster.getRegionThreads() != null) {
      for(Thread t: this.cluster.getRegionThreads()) {
        threadDumpingJoin(t);
      }
    }
    threadDumpingJoin(this.cluster.getMaster());
  }

  protected void threadDumpingJoin(final Thread t) {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.info("Continuing...", e);
      }
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }
}
