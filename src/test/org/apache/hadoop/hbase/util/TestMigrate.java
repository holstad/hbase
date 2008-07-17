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

package org.apache.hadoop.hbase.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;

/**
 * Runs migration of filesystem from hbase 0.1 to 0.2.
 */
public class TestMigrate extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestMigrate.class);
  
  // This is the name of the table that is in the data file.
  private static final String TABLENAME = "TestUpgrade";
  
  // The table has two columns
  private static final byte [][] TABLENAME_COLUMNS =
    {Bytes.toBytes("column_a:"), Bytes.toBytes("column_b:")};

  // Expected count of rows in migrated table.
  private static final int EXPECTED_COUNT = 17576;

  /**
   * Test migration
   * @throws IOException 
   */
  public void testUpgrade() throws IOException {
    MiniDFSCluster dfsCluster = null;
    try {
      dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR, new Path(
        dfsCluster.getFileSystem().getHomeDirectory(), "hbase").toString());
      FileSystem dfs = dfsCluster.getFileSystem();
      Path rootDir =
        dfs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
      dfs.mkdirs(rootDir);
      loadTestData(dfs, rootDir);
      listPaths(dfs, rootDir, rootDir.toString().length() + 1);
      
      Migrate u = new Migrate(conf);
      u.run(new String[] {"check"});
      listPaths(dfs, rootDir, rootDir.toString().length() + 1);
      
      u = new Migrate(conf);
      u.run(new String[] {"upgrade"});
      listPaths(dfs, rootDir, rootDir.toString().length() + 1);
      
      // Try again. No upgrade should be necessary
      u = new Migrate(conf);
      u.run(new String[] {"check"});
      u = new Migrate(conf);
      u.run(new String[] {"upgrade"});
      
      // Now verify that can read contents.
      verify();
    } finally {
      if (dfsCluster != null) {
        shutdownDfs(dfsCluster);
      }
    }
  }
  
  /*
   * Load up test data.
   * @param dfs
   * @param rootDir
   * @throws IOException
   */
  private void loadTestData(final FileSystem dfs, final Path rootDir)
  throws IOException {
    FileSystem localfs = FileSystem.getLocal(conf);
    // Get path for zip file.  If running this test in eclipse, define
    // the system property src.testdata for your test run.
    String srcTestdata = System.getProperty("src.testdata");
    if (srcTestdata == null) {
      throw new NullPointerException("Define src.test system property");
    }
    Path data = new Path(srcTestdata, "HADOOP-2478-testdata-v0.1.zip");
    if (!localfs.exists(data)) {
      throw new FileNotFoundException(data.toString());
    }
    FSDataInputStream hs = localfs.open(data);
    ZipInputStream zip = new ZipInputStream(hs);
    unzip(zip, dfs, rootDir);
    zip.close();
    hs.close();
  }

  /*
   * Verify can read the migrated table.
   * @throws IOException
   */
  private void verify() throws IOException {
    // Delete any cached connections.  Need to do this because connection was
    // created earlier when no master was around.  The fact that there was no
    // master gets cached.  Need to delete so we go get master afresh.
    HConnectionManager.deleteConnectionInfo();
    
    LOG.info("Start a cluster against migrated FS");
    // Up number of retries.  Needed while cluster starts up. Its been set to 1
    // above.
    final int retries = 5;
    this.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER_KEY, retries);
    
    MiniHBaseCluster cluster = new MiniHBaseCluster(this.conf, 1);
    try {
      HBaseAdmin hb = new HBaseAdmin(this.conf);
      assertTrue(hb.isMasterRunning());
      HTableDescriptor [] tables = hb.listTables();
      boolean foundTable = false;
      for (int i = 0; i < tables.length; i++) {
        if (Bytes.equals(Bytes.toBytes(TABLENAME), tables[i].getName())) {
          foundTable = true;
          break;
        }
      }
      assertTrue(foundTable);
      LOG.info(TABLENAME + " exists.  Now waiting till startcode " +
        "changes before opening a scanner");
      waitOnStartCodeChange(retries);
      // Delete again so we go get it all fresh.
      HConnectionManager.deleteConnectionInfo();
      HTable t = new HTable(this.conf, TABLENAME);
      int count = 0;
      LOG.info("OPENING SCANNER");
      Scanner s = t.getScanner(TABLENAME_COLUMNS);
      try {
        for (RowResult r: s) {
          if (r == null || r.size() == 0) {
            break;
          }
          count++;
          if (count % 1000 == 0 && count > 0) {
            LOG.info("Iterated over " + count + " rows.");
          }
        }
        assertEquals(EXPECTED_COUNT, count);
      } finally {
        s.close();
      }
    } finally {
      HConnectionManager.deleteConnectionInfo();
      cluster.shutdown();
    }
  }

  /*
   * Wait till the startcode changes before we put up a scanner.  Otherwise
   * we tend to hang, at least on hudson and I've had it time to time on
   * my laptop.  The hang is down in RPC Client doing its call.  It
   * never returns though the socket has a read timeout of 60 seconds by
   * default. St.Ack
   * @param retries How many retries to run.
   * @throws IOException
   */
  private void waitOnStartCodeChange(final int retries) throws IOException {
    HTable m = new HTable(this.conf, HConstants.META_TABLE_NAME);
    // This is the start code that is in the old data.
    long oldStartCode = 1199736332062L;
    // This is the first row for the TestTable that is in the old data.
    byte [] row = Bytes.toBytes("TestUpgrade,,1199736362468");
    long pause = conf.getLong("hbase.client.pause", 5 * 1000);
    long startcode = -1;
    boolean changed = false;
    for (int i = 0; i < retries; i++) {
      startcode = Writables.cellToLong(m.get(row, HConstants.COL_STARTCODE));
      if (startcode != oldStartCode) {
        changed = true;
        break;
      }
      if ((i + 1) != retries) {
        try {
          Thread.sleep(pause);
        } catch (InterruptedException e) {
          // continue
        }
      }
    }
    // If after all attempts startcode has not changed, fail.
    if (!changed) {
      throw new IOException("Startcode didn't change after " + retries +
        " attempts");
    }
  }

  private void unzip(ZipInputStream zip, FileSystem dfs, Path rootDir)
  throws IOException {
    ZipEntry e = null;
    while ((e = zip.getNextEntry()) != null)  {
      if (e.isDirectory()) {
        dfs.mkdirs(new Path(rootDir, e.getName()));
      } else {
        FSDataOutputStream out = dfs.create(new Path(rootDir, e.getName()));
        byte[] buffer = new byte[4096];
        int len;
        do {
          len = zip.read(buffer);
          if (len > 0) {
            out.write(buffer, 0, len);
          }
        } while (len > 0);
        out.close();
      }
      zip.closeEntry();
    }
  }
  
  private void listPaths(FileSystem filesystem, Path dir, int rootdirlength)
  throws IOException {
    FileStatus[] stats = filesystem.listStatus(dir);
    if (stats == null || stats.length == 0) {
      return;
    }
    for (int i = 0; i < stats.length; i++) {
      String path = stats[i].getPath().toString();
      if (stats[i].isDir()) {
        System.out.println("d " + path);
        listPaths(filesystem, stats[i].getPath(), rootdirlength);
      } else {
        System.out.println("f " + path + " size=" + stats[i].getLen());
      }
    }
  }
}