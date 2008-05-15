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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.DistributedFileSystem;
import org.apache.hadoop.dfs.FSConstants;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.HbaseRPC;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;

/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 */
public class HMaster extends Thread implements HConstants, HMasterInterface, 
  HMasterRegionInterface {
  
  static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  /** {@inheritDoc} */
  public long getProtocolVersion(String protocol,
      @SuppressWarnings("unused") long clientVersion)
  throws IOException {
    if (protocol.equals(HMasterInterface.class.getName())) {
      return HMasterInterface.versionID; 
    } else if (protocol.equals(HMasterRegionInterface.class.getName())) {
      return HMasterRegionInterface.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }

  // We start out with closed flag on.  Using AtomicBoolean rather than
  // plain boolean because want to pass a reference to supporting threads
  // started here in HMaster rather than have them have to know about the
  // hosting class
  volatile AtomicBoolean closed = new AtomicBoolean(true);
  volatile boolean shutdownRequested = false;
  volatile boolean fsOk = true;
  final Path rootdir;
  final HBaseConfiguration conf;
  final FileSystem fs;
  final Random rand;
  final int threadWakeFrequency; 
  final int numRetries;
  final long maxRegionOpenTime;
  final int leaseTimeout;

  volatile DelayQueue<RegionServerOperation> delayedToDoQueue =
    new DelayQueue<RegionServerOperation>();
  volatile BlockingQueue<RegionServerOperation> toDoQueue =
    new LinkedBlockingQueue<RegionServerOperation>();

  private final Server server;
  private final HServerAddress address;

  final HConnection connection;

  final int metaRescanInterval;
  
  // A Sleeper that sleeps for threadWakeFrequency
  protected final Sleeper sleeper;
  
  // Default access so accesible from unit tests. MASTER is name of the webapp
  // and the attribute name used stuffing this instance into web context.
  InfoServer infoServer;
  
  /** Name of master server */
  public static final String MASTER = "master";

  /** @return InfoServer object */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  ServerManager serverManager;
  RegionManager regionManager;
  
  /** Build the HMaster out of a raw configuration item.
   * 
   * @param conf - Configuration object
   * @throws IOException
   */
  public HMaster(HBaseConfiguration conf) throws IOException {
    this(new Path(conf.get(HBASE_DIR)),
        new HServerAddress(conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param rd base directory of this HBase instance.  Must be fully
   * qualified so includes filesystem to use.
   * @param address server address and port number
   * @param conf configuration
   * 
   * @throws IOException
   */
  public HMaster(Path rd, HServerAddress address, HBaseConfiguration conf)
  throws IOException {
    this.conf = conf;
    try {
      FSUtils.validateRootPath(rd);
    } catch (IOException e) {
      LOG.fatal("Not starting HMaster because the root directory path '" +
          rd.toString() + "' is not valid. Check the setting of the" +
          " configuration parameter '" + HBASE_DIR + "'", e);
      throw e;
    }
    this.rootdir = rd;
    this.threadWakeFrequency = conf.getInt(THREAD_WAKE_FREQUENCY, 10 * 1000);
    // The filesystem hbase wants to use is probably not what is set into
    // fs.default.name; its value is probably the default.
    this.conf.set("fs.default.name", this.rootdir.toString());
    this.fs = FileSystem.get(conf);
    if (this.fs instanceof DistributedFileSystem) {
      // Make sure dfs is not in safe mode
      String message = "Waiting for dfs to exit safe mode...";
      while (((DistributedFileSystem) fs).setSafeMode(
          FSConstants.SafeModeAction.SAFEMODE_GET)) {
        LOG.info(message);
        try {
          Thread.sleep(this.threadWakeFrequency);
        } catch (InterruptedException e) {
          //continue
        }
      }
    }
    this.conf.set(HConstants.HBASE_DIR, this.rootdir.toString());
    this.rand = new Random();
    Path rootRegionDir =
      HRegion.getRegionDir(rootdir, HRegionInfo.ROOT_REGIONINFO);
    LOG.info("Root region dir: " + rootRegionDir.toString());

    try {
      // Make sure the root directory exists!
      if (!fs.exists(rootdir)) {
        fs.mkdirs(rootdir); 
        FSUtils.setVersion(fs, rootdir);
      } else {
        FSUtils.checkVersion(fs, rootdir, true);
      }

      if (!fs.exists(rootRegionDir)) {
        LOG.info("BOOTSTRAP: creating ROOT and first META regions");
        try {
          HRegion root = HRegion.createHRegion(HRegionInfo.ROOT_REGIONINFO,
            this.rootdir, this.conf);
          HRegion meta = HRegion.createHRegion(HRegionInfo.FIRST_META_REGIONINFO,
            this.rootdir, this.conf);

          // Add first region from the META table to the ROOT region.
          HRegion.addRegionToMETA(root, meta);
          root.close();
          root.getLog().closeAndDelete();
          meta.close();
          meta.getLog().closeAndDelete();
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.error("bootstrap", e);
          throw e;
        }
      }
    } catch (IOException e) {
      LOG.fatal("Not starting HMaster because:", e);
      throw e;
    }

    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime =
      conf.getLong("hbase.hbasemaster.maxregionopen", 60 * 1000);
    this.leaseTimeout = conf.getInt("hbase.master.lease.period", 30 * 1000);
    
    this.server = HbaseRPC.getServer(this, address.getBindAddress(),
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

    //  The rpc-server port can be ephemeral... ensure we have the correct info
    this.address = new HServerAddress(server.getListenerAddress());
    conf.set(MASTER_ADDRESS, address.toString());

    this.connection = HConnectionManager.getConnection(conf);

    this.metaRescanInterval =
      conf.getInt("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    this.sleeper = new Sleeper(this.threadWakeFrequency, this.closed);
    
    serverManager = new ServerManager(this);
    regionManager = new RegionManager(this);
    
    // We're almost open for business
    this.closed.set(false);
    LOG.info("HMaster initialized on " + this.address.toString());
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  protected boolean checkFileSystem() {
    if (fsOk) {
      try {
        FSUtils.checkFileSystemAvailable(fs);
      } catch (IOException e) {
        LOG.fatal("Shutting down HBase cluster: file system not available", e);
        closed.set(true);
        fsOk = false;
      }
    }
    return fsOk;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return address;
  }
  
  /**
   * @return Hbase root dir.
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  /**
   * @return Read-only map of servers to serverinfo.
   */
  public Map<String, HServerInfo> getServersToServerInfo() {
    return serverManager.getServersToServerInfo();
  }

  /**
   * @return Read-only map of servers to load.
   */
  public Map<String, HServerLoad> getServersToLoad() {
    return serverManager.getServersToLoad();
  }
  
  /** @return The average load */
  public double getAverageLoad() {
    return serverManager.getAverageLoad();
  }

  /**
   * @return Location of the <code>-ROOT-</code> region.
   */
  public HServerAddress getRootRegionLocation() {
    HServerAddress rootServer = null;
    if (!shutdownRequested && !closed.get()) {
      rootServer = regionManager.getRootRegionLocation();
    }
    return rootServer;
  }
  
  /**
   * Wait until root region is available
   */
  public void waitForRootRegionLocation() {
    regionManager.waitForRootRegionLocation();
  }
  
  /**
   * @return Read-only map of online regions.
   */
  public Map<byte [], MetaRegion> getOnlineMetaRegions() {
    return regionManager.getOnlineMetaRegions();
  }

  /** Main processing loop */
  @Override
  public void run() {
    final String threadName = "HMaster";
    Thread.currentThread().setName(threadName);
    startServiceThreads();
    /* Main processing loop */
    try {
      while (!closed.get()) {
        // check if we should be shutting down
        if (shutdownRequested && serverManager.numServers() == 0) {
          startShutdown();
          break;
        }
        
        // work on the TodoQueue. If that fails, we should shut down.
        if (!processToDoQueue()) {
          break;
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting shutdown.", t);
      closed.set(true);
    }
    // The region servers won't all exit until we stop scanning the meta regions
    regionManager.stopScanners();
    
    // Wait for all the remaining region servers to report in.
    serverManager.letRegionServersShutdown();

    /*
     * Clean up and close up shop
     */
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (InterruptedException ex) {
        ex.printStackTrace();
      }
    }
    server.stop();                      // Stop server
    serverManager.stop();
    regionManager.stop();
    
    // Join up with all threads
    LOG.info("HMaster main thread exiting");
  }
  
  /**
   * Try to get an operation off of the todo queue and perform it.
   */ 
  private boolean processToDoQueue() {
    RegionServerOperation op = null;
    
    // block until the root region is online
    if (regionManager.getRootRegionLocation() != null) {
      // We can't process server shutdowns unless the root region is online
      op = delayedToDoQueue.poll();
    }
    
    // if there aren't any todo items in the queue, sleep for a bit.
    if (op == null ) {
      try {
        op = toDoQueue.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // continue
      }
    }
    
    // at this point, if there's still no todo operation, or we're supposed to
    // be closed, return.
    if (op == null || closed.get()) {
      return true;
    }
    
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Main processing loop: " + op.toString());
      }
      
      // perform the operation. 
      if (!op.process()) {
        // Operation would have blocked because not all meta regions are
        // online. This could cause a deadlock, because this thread is waiting
        // for the missing meta region(s) to come back online, but since it
        // is waiting, it cannot process the meta region online operation it
        // is waiting for. So put this operation back on the queue for now.
        if (toDoQueue.size() == 0) {
          // The queue is currently empty so wait for a while to see if what
          // we need comes in first
          sleeper.sleep();
        }
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Put " + op.toString() + " back on queue");
          }
          toDoQueue.put(op);
        } catch (InterruptedException e) {
          throw new RuntimeException(
            "Putting into toDoQueue was interrupted.", e);
        }
      }
    } catch (Exception ex) {
      // There was an exception performing the operation.
      if (ex instanceof RemoteException) {
        try {
          ex = RemoteExceptionHandler.decodeRemoteException(
            (RemoteException)ex);
        } catch (IOException e) {
          ex = e;
          LOG.warn("main processing loop: " + op.toString(), e);
        }
      }
      // make sure the filesystem is still ok. otherwise, we're toast.
      if (!checkFileSystem()) {
        return false;
      }
      LOG.warn("Processing pending operations: " + op.toString(), ex);
      try {
        // put the operation back on the queue... maybe it'll work next time.
        toDoQueue.put(op);
      } catch (InterruptedException e) {
        throw new RuntimeException(
          "Putting into toDoQueue was interrupted.", e);
      } catch (Exception e) {
        LOG.error("main processing loop: " + op.toString(), e);
      }
    }
    return true;
  }
  
  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() {
    try {
      regionManager.start();
      serverManager.start();
      // Put up info server.
      int port = this.conf.getInt("hbase.master.info.port", 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
      }
      // Start the server so everything else is running before we start
      // receiving requests.
      this.server.start();
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        } catch (IOException ex) {
          LOG.warn("thread start", ex);
        }
      }
      // Something happened during startup. Shut things down.
      this.closed.set(true);
      LOG.error("Failed startup", e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }
  }

  /*
   * Start shutting down the master
   */
  void startShutdown() {
    closed.set(true);
    regionManager.stopScanners();
    synchronized(toDoQueue) {
      toDoQueue.clear();                         // Empty the queue
      delayedToDoQueue.clear();                  // Empty shut down queue
      toDoQueue.notifyAll();                     // Wake main thread
    }
    serverManager.notifyServers();
  }

  /*
   * HMasterRegionInterface
   */

  /** {@inheritDoc} */
  @SuppressWarnings("unused")
  public MapWritable regionServerStartup(HServerInfo serverInfo)
  throws IOException {
    // register with server manager
    serverManager.regionServerStartup(serverInfo);
    // send back some config info
    return createConfigurationSubset();
  }
  
  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  /** {@inheritDoc} */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[], 
    HRegionInfo[] mostLoadedRegions)
  throws IOException {
    return serverManager.regionServerReport(serverInfo, msgs, 
      mostLoadedRegions);
  }

  /*
   * HMasterInterface
   */

  /** {@inheritDoc} */
  public boolean isMasterRunning() {
    return !closed.get();
  }

  /** {@inheritDoc} */
  public void shutdown() {
    LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    this.shutdownRequested = true;
  }

  /** {@inheritDoc} */
  public void createTable(HTableDescriptor desc)
  throws IOException {    
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(desc, null, null);

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not access meta regions if they have not already been
        // assigned and scanned.  If we timeout waiting, just shutdown.
        if (regionManager.waitForMetaRegionsOrClose()) {
          break;
        }
        createTable(newRegion);
        LOG.info("created table " + desc.getNameAsString());
        break;
      } catch (TableExistsException e) {
        throw e;
      } catch (IOException e) {
        if (tries == numRetries - 1) {
          throw RemoteExceptionHandler.checkIOException(e);
        }
        sleeper.sleep();
      }
    }
  }

  private synchronized void createTable(final HRegionInfo newRegion) 
  throws IOException {
    byte [] tableName = newRegion.getTableDesc().getName();
    // 1. Check to see if table already exists. Get meta region where
    // table would sit should it exist. Open scanner on it. If a region
    // for the table we want to create already exists, then table already
    // created. Throw already-exists exception.
    MetaRegion m = regionManager.getFirstMetaRegionForRegion(newRegion);
        
    byte [] metaRegionName = m.getRegionName();
    HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
    long scannerid = srvr.openScanner(metaRegionName, COL_REGIONINFO_ARRAY,
      tableName, LATEST_TIMESTAMP, null);
    try {
      RowResult data = srvr.next(scannerid);
          
      // Test data and that the row for the data is for our table. If table
      // does not exist, scanner will return row after where our table would
      // be inserted if it exists so look for exact match on table name.            
      if (data != null && data.size() > 0) {
        byte [] tn = HRegionInfo.getTableNameFromRegionName(data.getRow());
        if (Bytes.equals(tn, tableName)) {
          // Then a region for this table already exists. Ergo table exists.
          throw new TableExistsException(Bytes.toString(tableName));
        }
      }
    } finally {
      srvr.close(scannerid);
    }
    regionManager.createRegion(newRegion, srvr, metaRegionName);
  }

  /** {@inheritDoc} */
  public void deleteTable(final byte [] tableName) throws IOException {
    new TableDelete(this, tableName).process();
    LOG.info("deleted table: " + Bytes.toString(tableName));
  }

  /** {@inheritDoc} */
  public void addColumn(byte [] tableName, HColumnDescriptor column)
  throws IOException {    
    new AddColumn(this, tableName, column).process();
  }

  /** {@inheritDoc} */
  public void modifyColumn(byte [] tableName, byte [] columnName, 
    HColumnDescriptor descriptor)
  throws IOException {
    new ModifyColumn(this, tableName, columnName, descriptor).process();
  }

  /** {@inheritDoc} */
  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    new DeleteColumn(this, tableName, HStoreKey.getFamily(c)).process();
  }

  /** {@inheritDoc} */
  public void enableTable(final byte [] tableName) throws IOException {
    new ChangeTableState(this, tableName, true).process();
  }

  /** {@inheritDoc} */
  public void disableTable(final byte [] tableName) throws IOException {
    new ChangeTableState(this, tableName, false).process();
  }

  /** {@inheritDoc} */
  public HServerAddress findRootRegion() {
    return regionManager.getRootRegionLocation();
  }

  /*
   * Managing leases
   */

  /**
   * @return Return configuration being used by this server.
   */
  public HBaseConfiguration getConfiguration() {
    return this.conf;
  }
    
  /*
   * Get HRegionInfo from passed META map of row values.
   * Returns null if none found (and logs fact that expected COL_REGIONINFO
   * was missing).  Utility method used by scanners of META tables.
   * @param row name of the row
   * @param map Map to do lookup in.
   * @return Null or found HRegionInfo.
   * @throws IOException
   */
  HRegionInfo getHRegionInfo(final byte [] row, final Map<byte [], Cell> map)
  throws IOException {
    Cell regioninfo = map.get(COL_REGIONINFO);
    if (regioninfo == null) {
      StringBuilder sb =  new StringBuilder();
      for (byte [] e: map.keySet()) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(Bytes.toString(e));
      }
      LOG.warn(Bytes.toString(COL_REGIONINFO) + " is empty for row: " +
         Bytes.toString(row) + "; has keys: " + sb.toString());
      return null;
    }
    return Writables.getHRegionInfo(regioninfo.getValue());
  }

  /*
   * When we find rows in a meta region that has an empty HRegionInfo, we
   * clean them up here.
   * 
   * @param s connection to server serving meta region
   * @param metaRegionName name of the meta region we scanned
   * @param emptyRows the row keys that had empty HRegionInfos
   */
  protected void deleteEmptyMetaRows(HRegionInterface s, 
      byte [] metaRegionName,
      List<byte []> emptyRows) {
    for (byte [] regionName: emptyRows) {
      try {
        HRegion.removeRegionFromMETA(s, metaRegionName, regionName);
        LOG.warn("Removed region: " + Bytes.toString(regionName) +
          " from meta region: " +
          Bytes.toString(metaRegionName) + " because HRegionInfo was empty");
      } catch (IOException e) {
        LOG.error("deleting region: " + regionName + " from meta region: " +
            metaRegionName, e);
      }
    }
  }
   
  /*
   * Main program
   */

  private static void printUsageAndExit() {
    System.err.println("Usage: java org.apache.hbase.HMaster " +
    "[--bind=hostname:port] start|stop");
    System.exit(0);
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {

    if (args.length < 1) {
      printUsageAndExit();
    }

    HBaseConfiguration conf = new HBaseConfiguration();

    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).

    final String addressArgKey = "--bind=";
    for (String cmd: args) {
      if (cmd.startsWith(addressArgKey)) {
        conf.set(MASTER_ADDRESS, cmd.substring(addressArgKey.length()));
        continue;
      }

      if (cmd.equals("start")) {
        try {
          // If 'local', defer to LocalHBaseCluster instance.
          if (LocalHBaseCluster.isLocal(conf)) {
            (new LocalHBaseCluster(conf)).startup();
          } else {
            Constructor<? extends HMaster> c =
              masterClass.getConstructor(HBaseConfiguration.class);
            HMaster master = c.newInstance(conf);
            master.start();
          }
        } catch (Throwable t) {
          LOG.error( "Can not start master", t);
          System.exit(-1);
        }
        break;
      }

      if (cmd.equals("stop")) {
        try {
          if (LocalHBaseCluster.isLocal(conf)) {
            LocalHBaseCluster.doLocal(conf);
          }
          HBaseAdmin adm = new HBaseAdmin(conf);
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error( "Can not stop master", t);
          System.exit(-1);
        }
        break;
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }
  
  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HMaster.class);
  }
}
