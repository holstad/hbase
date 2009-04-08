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

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Perform a migration.
 * HBase keeps a file in hdfs named hbase.version just under the hbase.rootdir.
 * This file holds the version of the hbase data in the Filesystem.  When the
 * software changes in a manner incompatible with the data in the Filesystem,
 * it updates its internal version number,
 * {@link HConstants#FILE_SYSTEM_VERSION}.  This wrapper script manages moving
 * the filesystem across versions until there's a match with current software's
 * version number.  This script will only cross a particular version divide.  You may
 * need to install earlier or later hbase to migrate earlier (or older) versions.
 * 
 * <p>This wrapper script comprises a set of migration steps.  Which steps
 * are run depends on the span between the version of the hbase data in the
 * Filesystem and the version of the current softare.
 * 
 * <p>A migration script must accompany any patch that changes data formats.
 * 
 * <p>This script has a 'check' and 'execute' mode.  Adding migration steps,
 * its important to keep this in mind.  Testing if migration needs to be run,
 * be careful not to make presumptions about the current state of the data in
 * the filesystem.  It may have a format from many versions previous with
 * layout not as expected or keys and values of a format not expected.  Tools
 * such as {@link MetaUtils} may not work as expected when running against
 * old formats -- or, worse, fail in ways that are hard to figure (One such is
 * edits made by previous migration steps not being apparent in later migration
 * steps).  The upshot is always verify presumptions migrating.
 * 
 * <p>This script will migrate an hbase 0.18.x only.
 * 
 * @see <a href="http://wiki.apache.org/hadoop/Hbase/HowToMigrate">How To Migration</a>
 */
public class Migrate extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Migrate.class);
  private final HBaseConfiguration conf;
  private FileSystem fs;
  
  // Gets set by migration methods if we are in readOnly mode.
  boolean migrationNeeded = false;

  boolean readOnly = false;

  // Filesystem version of hbase 0.1.x.
  private static final float HBASE_0_1_VERSION = 0.1f;
  
  // Filesystem version we can migrate from
  private static final int PREVIOUS_VERSION = 4;
  
  private static final String MIGRATION_LINK = 
    " See http://wiki.apache.org/hadoop/Hbase/HowToMigrate for more information.";

  /** default constructor */
  public Migrate() {
    this(new HBaseConfiguration());
  }
  
  /**
   * @param conf
   */
  public Migrate(HBaseConfiguration conf) {
    super(conf);
    this.conf = conf;
  }
  
  /*
   * Sets the hbase rootdir as fs.default.name.
   * @return True if succeeded.
   */
  private boolean setFsDefaultName() {
    // Validate root directory path
    Path rd = new Path(conf.get(HConstants.HBASE_DIR));
    try {
      // Validate root directory path
      FSUtils.validateRootPath(rd);
    } catch (IOException e) {
      LOG.fatal("Not starting migration because the root directory path '" +
          rd.toString() + "' is not valid. Check the setting of the" +
          " configuration parameter '" + HConstants.HBASE_DIR + "'", e);
      return false;
    }
    this.conf.set("fs.default.name", rd.toString());
    return true;
  }

  /*
   * @return True if succeeded verifying filesystem.
   */
  private boolean verifyFilesystem() {
    try {
      // Verify file system is up.
      fs = FileSystem.get(conf);                        // get DFS handle
      LOG.info("Verifying that file system is available..");
      FSUtils.checkFileSystemAvailable(fs);
      return true;
    } catch (IOException e) {
      LOG.fatal("File system is not available", e);
      return false;
    }
  }
  
  private boolean notRunning() {
    // Verify HBase is down
    LOG.info("Verifying that HBase is not running...." +
          "Trys ten times  to connect to running master");
    try {
      HBaseAdmin.checkHBaseAvailable(conf);
      LOG.fatal("HBase cluster must be off-line.");
      return false;
    } catch (MasterNotRunningException e) {
      return true;
    }
  }
  
  public int run(String[] args) {
    if (parseArgs(args) != 0) {
      return -1;
    }
    if (!setFsDefaultName()) {
      return -2;
    }
    if (!verifyFilesystem()) {
      return -3;
    }
    if (!notRunning()) {
      return -4;
    }

    try {
      LOG.info("Starting upgrade" + (readOnly ? " check" : ""));

      // See if there is a file system version file
      String versionStr = FSUtils.getVersion(fs, FSUtils.getRootDir(this.conf));
      if (versionStr == null) {
        throw new IOException("File system version file " +
            HConstants.VERSION_FILE_NAME +
            " does not exist. No upgrade possible." + MIGRATION_LINK);
      }
      if (versionStr.compareTo(HConstants.FILE_SYSTEM_VERSION) == 0) {
        LOG.info("No upgrade necessary.");
        return 0;
      }
      float version = Float.parseFloat(versionStr);
      if (version == HBASE_0_1_VERSION ||
          Integer.valueOf(versionStr).intValue() < PREVIOUS_VERSION) {
        String msg = "Cannot upgrade from " + versionStr + " to " +
        HConstants.FILE_SYSTEM_VERSION + " you must install hbase-0.2.x, run " +
        "the upgrade tool, reinstall this version and run this utility again." +
        MIGRATION_LINK;
        System.out.println(msg);
        throw new IOException(msg);
      }

      migrate4To6();

      if (!readOnly) {
        // Set file system version
        LOG.info("Setting file system version.");
        FSUtils.setVersion(fs, FSUtils.getRootDir(this.conf));
        LOG.info("Upgrade successful.");
      } else if (this.migrationNeeded) {
        LOG.info("Upgrade needed.");
      }
      return 0;
    } catch (Exception e) {
      LOG.fatal("Upgrade" +  (readOnly ? " check" : "") + " failed", e);
      return -1;
    }
  }
  
  // Move the fileystem version from 4 to 6.
  // In here we rewrite the catalog table regions so they keep 10 versions
  // instead of 1.
  private void migrate4To6() throws IOException {
    if (this.readOnly && this.migrationNeeded) {
      return;
    }
    final MetaUtils utils = new MetaUtils(this.conf);
    try {
      // These two operations are effectively useless.  -ROOT- is hardcode,
      // at least until hbase 0.20.0 when we store it out in ZK.
      updateVersions(utils.getRootRegion().getRegionInfo());
      enableBlockCache(utils.getRootRegion().getRegionInfo());
      // Scan the root region
      utils.scanRootRegion(new MetaUtils.ScannerListener() {
        public boolean processRow(HRegionInfo info)
        throws IOException {
          if (readOnly && !migrationNeeded) {
            migrationNeeded = true;
            return false;
          }
          updateVersions(utils.getRootRegion(), info);
          enableBlockCache(utils.getRootRegion(), info);
          return true;
        }
      });
    } finally {
      utils.shutdown();
    }
  }

  /*
   * Enable blockcaching on catalog tables.
   * @param mr
   * @param oldHri
   */
  void enableBlockCache(HRegion mr, HRegionInfo oldHri)
  throws IOException {
    if (!enableBlockCache(oldHri)) {
      return;
    }
    BatchUpdate b = new BatchUpdate(oldHri.getRegionName());
    b.put(HConstants.COL_REGIONINFO, Writables.getBytes(oldHri));
    mr.batchUpdate(b);
    LOG.info("Enabled blockcache on " + oldHri.getRegionNameAsString());
  }

  /*
   * @param hri Update versions.
   * @param true if we changed value
   */
  private boolean enableBlockCache(final HRegionInfo hri) {
    boolean result = false;
    HColumnDescriptor hcd =
      hri.getTableDesc().getFamily(HConstants.COLUMN_FAMILY);
    if (hcd == null) {
      LOG.info("No info family in: " + hri.getRegionNameAsString());
      return result;
    }
    // Set blockcache enabled.
    hcd.setBlockCacheEnabled(true);
    return true;
  }


  /*
   * Update versions kept in historian.
   * @param mr
   * @param oldHri
   */
  void updateVersions(HRegion mr, HRegionInfo oldHri)
  throws IOException {
    if (!updateVersions(oldHri)) {
      return;
    }
    BatchUpdate b = new BatchUpdate(oldHri.getRegionName());
    b.put(HConstants.COL_REGIONINFO, Writables.getBytes(oldHri));
    mr.batchUpdate(b);
    LOG.info("Upped versions on " + oldHri.getRegionNameAsString());
  }

  /*
   * @param hri Update versions.
   * @param true if we changed value
   */
  private boolean updateVersions(final HRegionInfo hri) {
    boolean result = false;
    HColumnDescriptor hcd =
      hri.getTableDesc().getFamily(HConstants.COLUMN_FAMILY_HISTORIAN);
    if (hcd == null) {
      LOG.info("No region historian family in: " + hri.getRegionNameAsString());
      return result;
    }
    // Set historian records so they timeout after a week.
    if (hcd.getTimeToLive() == HConstants.FOREVER) {
      hcd.setTimeToLive(HConstants.WEEK_IN_SECONDS);
      result = true;
    }
    // Set the versions up to 10 from old default of 1.
    hcd = hri.getTableDesc().getFamily(HConstants.COLUMN_FAMILY);
    if (hcd.getMaxVersions() == 1) {
      // Set it to 10, an arbitrary high number
      hcd.setMaxVersions(10);
      result = true;
    }
    return result;
  }

  private int parseArgs(String[] args) {
    Options opts = new Options();
    GenericOptionsParser parser =
      new GenericOptionsParser(this.getConf(), opts, args);
    String[] remainingArgs = parser.getRemainingArgs();
    if (remainingArgs.length != 1) {
      usage();
      return -1;
    }
    if (remainingArgs[0].compareTo("check") == 0) {
      this.readOnly = true;
    } else if (remainingArgs[0].compareTo("upgrade") != 0) {
      usage();
      return -1;
    }
    return 0;
  }
  
  private void usage() {
    System.err.println("Usage: bin/hbase migrate {check | upgrade} [options]");
    System.err.println();
    System.err.println("  check                            perform upgrade checks only.");
    System.err.println("  upgrade                          perform upgrade checks and modify hbase.");
    System.err.println();
    System.err.println("  Options are:");
    System.err.println("    -conf <configuration file>     specify an application configuration file");
    System.err.println("    -D <property=value>            use value for given property");
    System.err.println("    -fs <local|namenode:port>      specify a namenode");
  }

  /**
   * Main program
   * 
   * @param args command line arguments
   */
  public static void main(String[] args) {
    int status = 0;
    try {
      status = ToolRunner.run(new Migrate(), args);
    } catch (Exception e) {
      LOG.error(e);
      status = -1;
    }
    System.exit(status);
  }
}