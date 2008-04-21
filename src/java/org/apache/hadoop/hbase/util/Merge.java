/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;

/**
 * Utility that can merge any two regions in the same table: adjacent,
 * overlapping or disjoint.
 */
public class Merge extends Configured implements Tool {
  static final Log LOG = LogFactory.getLog(Merge.class);
  private final HBaseConfiguration conf;
  private Path rootdir;
  private volatile MetaUtils utils;
  private Text tableName;               // Name of table
  private volatile Text region1;        // Name of region 1
  private volatile Text region2;        // Name of region 2
  private volatile boolean isMetaTable;
  private volatile HRegionInfo mergeInfo;

  /** default constructor */
  public Merge() {
    this(new HBaseConfiguration());
  }

  /**
   * @param conf
   */
  public Merge(HBaseConfiguration conf) {
    super(conf);
    this.conf = conf;
    this.mergeInfo = null;
  }

  /** {@inheritDoc} */
  public int run(String[] args) throws Exception {
    if (parseArgs(args) != 0) {
      return -1;
    }

    // Verify file system is up.
    FileSystem fs = FileSystem.get(this.conf);              // get DFS handle
    LOG.info("Verifying that file system is available...");
    try {
      FSUtils.checkFileSystemAvailable(fs);
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return -1;
    }
    
    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...");
    try {
      HBaseAdmin.checkHBaseAvailable(conf);
      LOG.fatal("HBase cluster must be off-line.");
      return -1;
    } catch (MasterNotRunningException e) {
      // Expected. Ignore.
    }
    
    // Initialize MetaUtils and and get the root of the HBase installation
    
    this.utils = new MetaUtils(conf);
    this.rootdir = utils.initialize();
    try {
      if (isMetaTable) {
        mergeTwoMetaRegions();
      } else {
        mergeTwoRegions();
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Merge failed", e);
      utils.scanMetaRegion(HRegionInfo.firstMetaRegionInfo,
          new MetaUtils.ScannerListener() {
            public boolean processRow(HRegionInfo info) {
              System.err.println(info.toString());
              return true;
            }
          }
      );

      return -1;
    
    } finally {
      if (this.utils != null && this.utils.isInitialized()) {
        this.utils.shutdown();
      }
    }
  }
  
  /** @return HRegionInfo for merge result */
  HRegionInfo getMergedHRegionInfo() {
    return this.mergeInfo;
  }

  /*
   * Merge two meta regions. This is unlikely to be needed soon as we have only
   * seend the meta table split once and that was with 64MB regions. With 256MB
   * regions, it will be some time before someone has enough data in HBase to
   * split the meta region and even less likely that a merge of two meta
   * regions will be needed, but it is included for completeness.
   */
  private void mergeTwoMetaRegions() throws IOException {
    HRegion rootRegion = utils.getRootRegion();
    HRegionInfo info1 = Writables.getHRegionInfo(
        rootRegion.get(region1, HConstants.COL_REGIONINFO));
    HRegionInfo info2 = Writables.getHRegionInfo(
        rootRegion.get(region2, HConstants.COL_REGIONINFO));
    HRegion merged = merge(info1, rootRegion, info2, rootRegion); 
    LOG.info("Adding " + merged.getRegionInfo() + " to " +
        rootRegion.getRegionInfo());
    HRegion.addRegionToMETA(rootRegion, merged);
    merged.close();
  }
  
  private static class MetaScannerListener
  implements MetaUtils.ScannerListener {
    private final Text region1;
    private final Text region2;
    private HRegionInfo meta1 = null;
    private HRegionInfo meta2 = null;
    
    MetaScannerListener(Text region1, Text region2) {
      this.region1 = region1;
      this.region2 = region2;
    }
    
    /** {@inheritDoc} */
    public boolean processRow(HRegionInfo info) {
      if (meta1 == null && HRegion.rowIsInRange(info, region1)) {
        meta1 = info;
      }
      if (region2 != null && meta2 == null &&
          HRegion.rowIsInRange(info, region2)) {
        meta2 = info;
      }
      return meta1 == null || (region2 != null && meta2 == null);
    }
    
    HRegionInfo getMeta1() {
      return meta1;
    }
    
    HRegionInfo getMeta2() {
      return meta2;
    }
  }
  
  /*
   * Merges two regions from a user table.
   */
  private void mergeTwoRegions() throws IOException {
    LOG.info("Merging regions " + this.region1.toString() + " and " +
      this.region2.toString() + " in table " + this.tableName.toString());
    // Scan the root region for all the meta regions that contain the regions
    // we're merging.
    MetaScannerListener listener = new MetaScannerListener(region1, region2);
    this.utils.scanRootRegion(listener);
    HRegionInfo meta1 = listener.getMeta1();
    if (meta1 == null) {
      throw new IOException("Could not find meta region for " + region1);
    }
    HRegionInfo meta2 = listener.getMeta2();
    if (meta2 == null) {
      throw new IOException("Could not find meta region for " + region2);
    }
    LOG.info("Found meta for region1 " + meta1.getRegionName() +
      ", meta for region2 " + meta2.getRegionName());
    HRegion metaRegion1 = this.utils.getMetaRegion(meta1);
    HRegionInfo info1 = Writables.getHRegionInfo(
      metaRegion1.get(region1, HConstants.COL_REGIONINFO));
    if (info1== null) {
      throw new NullPointerException("info1 is null using key " + region1 +
        " in " + meta1);
    }

    HRegion metaRegion2 = null;
    if (meta1.getRegionName().equals(meta2.getRegionName())) {
      metaRegion2 = metaRegion1;
    } else {
      metaRegion2 = utils.getMetaRegion(meta2);
    }
    HRegionInfo info2 = Writables.getHRegionInfo(
      metaRegion2.get(region2, HConstants.COL_REGIONINFO));
    if (info2 == null) {
      throw new NullPointerException("info2 is null using key " + meta2);
    }
    HRegion merged = merge(info1, metaRegion1, info2, metaRegion2);

    // Now find the meta region which will contain the newly merged region

    listener = new MetaScannerListener(merged.getRegionName(), null);
    utils.scanRootRegion(listener);
    HRegionInfo mergedInfo = listener.getMeta1();
    if (mergedInfo == null) {
      throw new IOException("Could not find meta region for " +
          merged.getRegionName());
    }
    HRegion mergeMeta = null;
    if (mergedInfo.getRegionName().equals(meta1.getRegionName())) {
      mergeMeta = metaRegion1;
    } else if (mergedInfo.getRegionName().equals(meta2.getRegionName())) {
      mergeMeta = metaRegion2;
    } else {
      mergeMeta = utils.getMetaRegion(mergedInfo);
    }
    LOG.info("Adding " + merged.getRegionInfo() + " to " +
        mergeMeta.getRegionInfo());

    HRegion.addRegionToMETA(mergeMeta, merged);
    merged.close();
  }
  
  /*
   * Actually merge two regions and update their info in the meta region(s)
   * If the meta is split, meta1 may be different from meta2. (and we may have
   * to scan the meta if the resulting merged region does not go in either)
   * Returns HRegion object for newly merged region
   */
  private HRegion merge(HRegionInfo info1, HRegion meta1, HRegionInfo info2,
      HRegion meta2)
  throws IOException {
    if (info1 == null) {
      throw new IOException("Could not find " + region1 + " in " +
          meta1.getRegionName());
    }
    if (info2 == null) {
      throw new IOException("Cound not find " + region2 + " in " +
          meta2.getRegionName());
    }
    HRegion merged = null;
    HLog log = utils.getLog();
    HRegion r1 = HRegion.openHRegion(info1, this.rootdir, log, this.conf);
    try {
      HRegion r2 = HRegion.openHRegion(info2, this.rootdir, log, this.conf);
      try {
        merged = HRegion.merge(r1, r2);
      } finally {
        if (!r2.isClosed()) {
          r2.close();
        }
      }
    } finally {
      if (!r1.isClosed()) {
        r1.close();
      }
    }
    
    // Remove the old regions from meta.
    // HRegion.merge has already deleted their files
    
    removeRegionFromMeta(meta1, info1);
    removeRegionFromMeta(meta2, info2);

    this.mergeInfo = merged.getRegionInfo();
    return merged;
  }
  
  /*
   * Removes a region's meta information from the passed <code>meta</code>
   * region.
   * 
   * @param meta META HRegion to be updated
   * @param regioninfo HRegionInfo of region to remove from <code>meta</code>
   *
   * @throws IOException
   */
  private void removeRegionFromMeta(HRegion meta, HRegionInfo regioninfo)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing region: " + regioninfo + " from " + meta);
    }
    meta.deleteAll(regioninfo.getRegionName(), System.currentTimeMillis());
  }

  /*
   * Adds a region's meta information from the passed <code>meta</code>
   * region.
   * 
   * @param metainfo META HRegionInfo to be updated
   * @param region HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  private int parseArgs(String[] args) {
    GenericOptionsParser parser =
      new GenericOptionsParser(this.getConf(), args);
    
    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 3) {
      usage();
      return -1;
    }
    tableName = new Text(remainingArgs[0]);
    isMetaTable = tableName.compareTo(HConstants.META_TABLE_NAME) == 0;
    
    region1 = new Text(remainingArgs[1]);
    region2 = new Text(remainingArgs[2]);
    int status = 0;
    // Why we duplicate code here? St.Ack
    if (WritableComparator.compareBytes(
        tableName.getBytes(), 0, tableName.getLength(),
        region1.getBytes(), 0, tableName.getLength()) != 0) {
      LOG.error("Region " + region1 + " does not belong to table " + tableName);
      status = -1;
    }
    if (WritableComparator.compareBytes(
        tableName.getBytes(), 0, tableName.getLength(),
        region2.getBytes(), 0, tableName.getLength()) != 0) {
      LOG.error("Region " + region2 + " does not belong to table " + tableName);
      status = -1;
    }
    if (region1.equals(region2)) {
      LOG.error("Can't merge a region with itself");
      status = -1;
    }
    return status;
  }
  
  private void usage() {
    System.err.println(
        "Usage: bin/hbase merge <table-name> <region-1> <region-2>\n");
  }
  
  /**
   * Main program
   * 
   * @param args
   */
  public static void main(String[] args) {
    int status = 0;
    try {
      status = ToolRunner.run(new Merge(), args);
    } catch (Exception e) {
      LOG.error("exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
