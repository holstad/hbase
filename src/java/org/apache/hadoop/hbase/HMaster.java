/**
 * Copyright 2006-7 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.StringUtils;

/**
 * HMaster is the "master server" for a HBase.
 * There is only one HMaster for a single HBase deployment.
 */
public class HMaster implements HConstants, HMasterInterface, 
    HMasterRegionInterface, Runnable {

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

  static final Log LOG =
    LogFactory.getLog(org.apache.hadoop.hbase.HMaster.class.getName());
  
  volatile boolean closed;
  Path dir;
  private Configuration conf;
  FileSystem fs;
  Random rand;
  private long threadWakeFrequency; 
  private int numRetries;
  private long maxRegionOpenTime;
  
  Vector<PendingOperation> msgQueue;
  
  private Leases serverLeases;
  private Server server;
  private HServerAddress address;
  
  HClient client;
 
  long metaRescanInterval;
  
  private HServerAddress rootRegionLocation;
  
  /**
   * Columns in the 'meta' ROOT and META tables.
   */
  static final Text METACOLUMNS[] = {
      COLUMN_FAMILY
  };
  
  static final String MASTER_NOT_RUNNING = "Master not running";

  boolean rootScanned;
  int numMetaRegions;

  /**
   * Base HRegion scanner class. Holds utilty common to <code>ROOT</code> and
   * <code>META</code> HRegion scanners.
   * 
   * <p>How do we know if all regions are assigned? After the initial scan of
   * the <code>ROOT</code> and <code>META</code> regions, all regions known at
   * that time will have been or are in the process of being assigned.</p>
   * 
   * <p>When a region is split the region server notifies the master of the
   * split and the new regions are assigned. But suppose the master loses the
   * split message? We need to periodically rescan the <code>ROOT</code> and
   * <code>META</code> regions.
   *    <ul>
   *    <li>If we rescan, any regions that are new but not assigned will have
   *    no server info. Any regions that are not being served by the same
   *    server will get re-assigned.</li>
   *      
   *    <li>Thus a periodic rescan of the root region will find any new
   *    <code>META</code> regions where we missed the <code>META</code> split
   *    message or we failed to detect a server death and consequently need to
   *    assign the region to a new server.</li>
   *        
   *    <li>if we keep track of all the known <code>META</code> regions, then
   *    we can rescan them periodically. If we do this then we can detect any
   *    regions for which we missed a region split message.</li>
   *    </ul>
   *    
   * Thus just keeping track of all the <code>META</code> regions permits
   * periodic rescanning which will detect unassigned regions (new or
   * otherwise) without the need to keep track of every region.</p>
   * 
   * <p>So the <code>ROOT</code> region scanner needs to wake up:
   * <ol>
   * <li>when the master receives notification that the <code>ROOT</code>
   * region has been opened.</li>
   * <li>periodically after the first scan</li>
   * </ol>
   * 
   * The <code>META</code>  scanner needs to wake up:
   * <ol>
   * <li>when a <code>META</code> region comes on line</li>
   * </li>periodically to rescan the known <code>META</code> regions</li>
   * </ol>
   * 
   * <p>A <code>META</code> region is not 'known' until it has been scanned
   * once.
   */
  abstract class BaseScanner implements Runnable {
    private final Text FIRST_ROW = new Text();
    
    /**
     * @param region Region to scan
     * @return True if scan completed.
     * @throws IOException
     */
    protected boolean scanRegion(final MetaRegion region)
    throws IOException {
      boolean scannedRegion = false;
      HRegionInterface regionServer = null;
      long scannerId = -1L;
      if (LOG.isDebugEnabled()) {
        LOG.debug(Thread.currentThread().getName() + " scanning meta region " +
          region.regionName);
      }

      try {
        regionServer = client.getHRegionConnection(region.server);
        scannerId = regionServer.openScanner(region.regionName, METACOLUMNS,
          FIRST_ROW);
        while (true) {
          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          HStoreKey key = new HStoreKey();
          LabelledData[] values = regionServer.next(scannerId, key);
          if (values.length == 0) {
            break;
          }
          
          for (int i = 0; i < values.length; i++) {
            byte[] bytes = new byte[values[i].getData().getSize()];
            System.arraycopy(values[i].getData().get(), 0, bytes, 0,
              bytes.length);
            results.put(values[i].getLabel(), bytes);
          }

          HRegionInfo info = HRegion.getRegionInfo(results);
          String serverName = HRegion.getServerName(results);
          long startCode = HRegion.getStartCode(results);

          if(LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + " scanner: " +
              Long.valueOf(scannerId) + " regioninfo: {" + info.toString() +
              "}, server: " + serverName + ", startCode: " + startCode);
          }

          // Note Region has been assigned.
          checkAssigned(info, serverName, startCode);
          scannedRegion = true;
        }
      } catch (UnknownScannerException e) {
        // Reset scannerId so we do not try closing a scanner the other side
        // has lost account of: prevents duplicated stack trace out of the 
        // below close in the finally.
        scannerId = -1L;
      } finally {
        try {
          if (scannerId != -1L) {
            if (regionServer != null) {
              regionServer.close(scannerId);
            }
          }
        } catch (IOException e) {
          LOG.error(e);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(Thread.currentThread().getName() + " scan of meta region " +
          region.regionName + " complete");
      }
      return scannedRegion;
    }
    
    protected void checkAssigned(final HRegionInfo info,
        final String serverName, final long startCode) {

      // Skip region - if ...
      if(info.offLine                                           // offline
          || killedRegions.contains(info.regionName)            // queued for offline
          || regionsToDelete.contains(info.regionName)) {       // queued for delete

        unassignedRegions.remove(info.regionName);
        assignAttempts.remove(info.regionName);
        
        if(LOG.isDebugEnabled()) {
          LOG.debug("not assigning region: " + info.regionName);
        }
        return;
      }
      
      HServerInfo storedInfo = null;
      if(serverName != null) {
        TreeMap<Text, HRegionInfo> regionsToKill = killList.get(serverName);
        if(regionsToKill != null && regionsToKill.containsKey(info.regionName)) {
          // Skip if region is on kill list
          
          if(LOG.isDebugEnabled()) {
            LOG.debug("not assigning region (on kill list): " + info.regionName);
          }
          return;
        }
        storedInfo = serversToServerInfo.get(serverName);
      }
      if(storedInfo == null || storedInfo.getStartCode() != startCode) {
        // The current assignment is no good; load the region.
        unassignedRegions.put(info.regionName, info);
        assignAttempts.put(info.regionName, 0L);
        if(LOG.isDebugEnabled()) {
          LOG.debug("region unassigned: " + info.regionName);
        }
      }
    }
  }
  
  /**
   * Scanner for the <code>ROOT</code> HRegion.
   */
  private class RootScanner extends BaseScanner {
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Running ROOT scanner");
      }
      try {
        while(!closed) {
          // rootRegionLocation will be filled in when we get an 'open region'
          // regionServerReport message from the HRegionServer that has been
          // allocated the ROOT region below.  If we get back false, then
          // HMaster has closed.
          if (waitForRootRegionOrClose()) {
            continue;
          }
          synchronized(rootScannerLock) { // Don't interrupt us while we're working
            rootScanned = false;
            // Make a MetaRegion instance for ROOT region to pass scanRegion.
            MetaRegion mr = new MetaRegion();
            mr.regionName = HGlobals.rootRegionInfo.regionName;
            mr.server = HMaster.this.rootRegionLocation;
            mr.startKey = null;
            if (scanRegion(mr)) {
              numMetaRegions += 1;
            }
            rootScanned = true;
          }
          try {
            Thread.sleep(metaRescanInterval);
          } catch(InterruptedException e) {
            // Catch and go around again. If interrupt, its spurious or we're
            // being shutdown.  Go back up to the while test.
          }
        }
      } catch(IOException e) {
        LOG.error("ROOT scanner", e);
        closed = true;
      }
      LOG.info("ROOT scanner exiting");
    }
  }
  
  private RootScanner rootScanner;
  private Thread rootScannerThread;
  private Integer rootScannerLock = 0;
  
  private static class MetaRegion implements Comparable {
    public HServerAddress server;
    public Text regionName;
    public Text startKey;

    @Override
    public boolean equals(Object o) {
      return this.compareTo(o) == 0;
    }
    
    @Override
    public int hashCode() {
      int result = this.regionName.hashCode();
      result ^= this.startKey.hashCode();
      return result;
    }

    // Comparable
    public int compareTo(Object o) {
      MetaRegion other = (MetaRegion)o;
      
      int result = this.regionName.compareTo(other.regionName);
      if(result == 0) {
        result = this.startKey.compareTo(other.startKey);
      }
      return result;
    }
    
  }
  
  /** Work for the meta scanner is queued up here */
  Vector<MetaRegion> metaRegionsToScan;

  SortedMap<Text, MetaRegion> knownMetaRegions;
  
  boolean allMetaRegionsScanned;
  
  /**
   * MetaScanner <code>META</code> table.
   * 
   * When a <code>META</code> server comes on line, a MetaRegion object is
   * queued up by regionServerReport() and this thread wakes up.
   *
   * It's important to do this work in a separate thread, or else the blocking 
   * action would prevent other work from getting done.
   */
  private class MetaScanner extends BaseScanner {
    @SuppressWarnings("null")
    public void run() {
      while (!closed) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running META scanner");
        }
        MetaRegion region = null;
        while (region == null && !closed) {
          synchronized (metaRegionsToScan) {
            if (metaRegionsToScan.size() != 0) {
              region = metaRegionsToScan.remove(0);
            }
            if (region == null) {
              try {
                metaRegionsToScan.wait();
              } catch (InterruptedException e) {
                // Catch and go around again.  We've been woken because there
                // are new meta regions available or because we are being
                // shut down.
              }
            }
          }
        }
        if (closed) {
          continue;
        }
        try {
          synchronized(metaScannerLock) { // Don't interrupt us while we're working
            scanRegion(region);
            knownMetaRegions.put(region.startKey, region);
            if (rootScanned && knownMetaRegions.size() == numMetaRegions) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("all meta regions scanned");
              }
              allMetaRegionsScanned = true;
              metaRegionsScanned();
            }
          }

          do {
            try {
              Thread.sleep(metaRescanInterval);
            } catch(InterruptedException ex) {
              // Catch and go around again.
            }
            if(!allMetaRegionsScanned         // A meta region must have split
                || closed) {                  // We're shutting down
              break;
            }

            // Rescan the known meta regions every so often

            synchronized(metaScannerLock) { // Don't interrupt us while we're working
              Vector<MetaRegion> v = new Vector<MetaRegion>();
              v.addAll(knownMetaRegions.values());
              for(Iterator<MetaRegion> i = v.iterator(); i.hasNext(); ) {
                scanRegion(i.next());
              }
            }
          } while(true);

        } catch(IOException e) {
          LOG.error("META scanner", e);
          closed = true;
        }
      }
      LOG.info("META scanner exiting");
    }

    private synchronized void metaRegionsScanned() {
      notifyAll();
    }
    
    public synchronized void waitForMetaScan() {
      while(!closed && !allMetaRegionsScanned) {
        try {
          wait();
        } catch(InterruptedException e) {
          // continue
        }
      }
    }
  }

  MetaScanner metaScanner;
  private Thread metaScannerThread;
  Integer metaScannerLock = 0;
  
  // The 'unassignedRegions' table maps from a region name to a HRegionInfo record,
  // which includes the region's table, its id, and its start/end keys.
  //
  // We fill 'unassignedRecords' by scanning ROOT and META tables, learning the 
  // set of all known valid regions.

  SortedMap<Text, HRegionInfo> unassignedRegions;

  // The 'assignAttempts' table maps from regions to a timestamp that indicates 
  // the last time we *tried* to assign the region to a RegionServer. If the 
  // timestamp is out of date, then we can try to reassign it.
  
  SortedMap<Text, Long> assignAttempts;

  SortedMap<String, TreeMap<Text, HRegionInfo>> killList;
  
  // 'killedRegions' contains regions that are in the process of being closed
  
  SortedSet<Text> killedRegions;
  
  // 'regionsToDelete' contains regions that need to be deleted, but cannot be
  // until the region server closes it
  
  SortedSet<Text> regionsToDelete;
  
  // A map of known server names to server info

  SortedMap<String, HServerInfo> serversToServerInfo =
    Collections.synchronizedSortedMap(new TreeMap<String, HServerInfo>());

  /** Build the HMaster out of a raw configuration item. */
  public HMaster(Configuration conf) throws IOException {
    this(new Path(conf.get(HREGION_DIR, DEFAULT_HREGION_DIR)),
        new HServerAddress(conf.get(MASTER_ADDRESS, DEFAULT_MASTER_ADDRESS)),
        conf);
  }

  /** 
   * Build the HMaster
   * @param dir         - base directory
   * @param address     - server address and port number
   * @param conf        - configuration
   * 
   * @throws IOException
   */
  public HMaster(Path dir, HServerAddress address, Configuration conf)
  throws IOException {
    this.closed = true;
    this.dir = dir;
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    this.rand = new Random();

    // Make sure the root directory exists!
    
    if(! fs.exists(dir)) {
      fs.mkdirs(dir);
    }

    Path rootRegionDir =
      HStoreFile.getHRegionDir(dir, HGlobals.rootRegionInfo.regionName);
    LOG.info("Root region dir: " + rootRegionDir.toString());
    if(! fs.exists(rootRegionDir)) {
      LOG.info("bootstrap: creating ROOT and first META regions");
      try {
        HRegion root = HRegion.createHRegion(0L, HGlobals.rootTableDesc,
          this.dir, this.conf);
        HRegion meta = HRegion.createHRegion(1L, HGlobals.metaTableDesc,
            this.dir, this.conf);
        // Add first region from the META table to the ROOT region.
        HRegion.addRegionToMETA(root, meta);
        root.close();
        root.getLog().close();
        meta.close();
        meta.getLog().close();
      } catch(IOException e) {
        LOG.error(e);
      }
    }

    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.maxRegionOpenTime = conf.getLong("hbase.hbasemaster.maxregionopen", 30 * 1000);
    this.msgQueue = new Vector<PendingOperation>();
    this.serverLeases = new Leases(
        conf.getLong("hbase.master.lease.period", 30 * 1000), 
        conf.getLong("hbase.master.lease.thread.wakefrequency", 15 * 1000));
    
    this.server = RPC.getServer(this, address.getBindAddress(),
        address.getPort(), conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);

    //  The rpc-server port can be ephemeral... ensure we have the correct info
    
    this.address = new HServerAddress(server.getListenerAddress());
    conf.set(MASTER_ADDRESS, address.toString());
    
    this.client = new HClient(conf);
    
    this.metaRescanInterval
      = conf.getLong("hbase.master.meta.thread.rescanfrequency", 60 * 1000);

    // The root region
    
    this.rootRegionLocation = null;
    this.rootScanned = false;
    this.rootScanner = new RootScanner();
    this.rootScannerThread = new Thread(rootScanner, "HMaster.rootScanner");
    
    // Scans the meta table

    this.numMetaRegions = 0;
    this.metaRegionsToScan = new Vector<MetaRegion>();
    
    this.knownMetaRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, MetaRegion>());
    
    this.allMetaRegionsScanned = false;

    this.metaScanner = new MetaScanner();
    this.metaScannerThread = new Thread(metaScanner, "HMaster.metaScanner");

    this.unassignedRegions = 
      Collections.synchronizedSortedMap(new TreeMap<Text, HRegionInfo>());
    
    this.unassignedRegions.put(HGlobals.rootRegionInfo.regionName, HGlobals.rootRegionInfo);
    
    this.assignAttempts = 
      Collections.synchronizedSortedMap(new TreeMap<Text, Long>());
    
    this.assignAttempts.put(HGlobals.rootRegionInfo.regionName, 0L);

    this.killList = 
      Collections.synchronizedSortedMap(
          new TreeMap<String, TreeMap<Text, HRegionInfo>>());
    
    this.killedRegions =
      Collections.synchronizedSortedSet(new TreeSet<Text>());
    
    this.regionsToDelete =
      Collections.synchronizedSortedSet(new TreeSet<Text>());
    
    // We're almost open for business
    
    this.closed = false;
    
    LOG.info("HMaster initialized on " + address.toString());
  }
  
  /** returns the HMaster server address */
  public HServerAddress getMasterAddress() {
    return address;
  }

  public void run() {
    Thread.currentThread().setName("HMaster");
    try { 
      // Start things up
      this.rootScannerThread.start();
      this.metaScannerThread.start();

      // Start the server last so everything else is running before we start
      // receiving requests
      this.server.start();
    } catch(IOException e) {
      // Something happened during startup. Shut things down.
      this.closed = true;
      LOG.error(e);
    }

    // Main processing loop
    for(PendingOperation op = null; !closed; ) {
      synchronized(msgQueue) {
        while(msgQueue.size() == 0 && !closed) {
          try {
            msgQueue.wait(threadWakeFrequency);
          } catch(InterruptedException iex) {
            // continue
          }
        }
        if(closed) {
          continue;
        }
        op = msgQueue.remove(msgQueue.size()-1);
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Processing " + op.toString());
        }
        op.process();
      } catch(Exception ex) {
        msgQueue.insertElementAt(op, 0);
      }
    }
    letRegionServersShutdown();
    
    /*
     * Clean up and close up shop
     */

    // Wake other threads so they notice the close

    synchronized(rootScannerLock) {
      rootScannerThread.interrupt();
    }
    synchronized(metaScannerLock) {
      metaScannerThread.interrupt();
    }
    server.stop();                              // Stop server
    serverLeases.close();                       // Turn off the lease monitor
    
    // Join up with all threads
    
    try {
      // Wait for the root scanner to finish.
      rootScannerThread.join();
    } catch(Exception iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    try {
      // Join the thread till it finishes.
      metaScannerThread.join();
    } catch(Exception iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    try {
      // Join until its finished.  TODO: Maybe do in parallel in its own thread
      // as is done in TaskTracker if its taking a long time to go down.
      server.join();
    } catch(InterruptedException iex) {
      // Print if ever there is an interrupt (Just for kicks. Remove if it
      // ever happens).
      LOG.warn(iex);
    }
    
    LOG.info("HMaster main thread exiting");
  }
  
  /**
   * Wait on regionservers to report in.  Then, they notice the HMaster
   * is going down and will shut themselves down.
   */
  private void letRegionServersShutdown() {
    long regionServerMsgInterval =
      conf.getLong("hbase.regionserver.msginterval", 15 * 1000);
    // Wait for 3 * hbase.regionserver.msginterval intervals or until all
    // regionservers report in as closed.
    long endTime = System.currentTimeMillis() + (regionServerMsgInterval * 3);
    while (endTime > System.currentTimeMillis() &&
        this.serversToServerInfo.size() > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting on regionservers: " + this.serversToServerInfo);
      }
      try {
        Thread.sleep(threadWakeFrequency);
      } catch (InterruptedException e) {
        // Ignore interrupt.
      }
    }
  }
  
  /**
   * Wait until <code>rootRegionLocation</code> has been set or until the
   * <code>closed</code> flag has been set.
   * @return True if <code>rootRegionLocation</code> was populated.
   */
  private synchronized boolean waitForRootRegionOrClose() {
    while (!closed && rootRegionLocation == null) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wait for root region (or close)");
        }
        wait();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wake from wait for root region (or close)");
        }
      } catch(InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wake from wait for root region (or close) (IE)");
        }
      }
    }
    return this.rootRegionLocation == null;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // HMasterRegionInterface
  //////////////////////////////////////////////////////////////////////////////
  
  /** HRegionServers call this method upon startup. */
  public void regionServerStartup(HServerInfo serverInfo) throws IOException {
    String server = serverInfo.getServerAddress().toString().trim();
    HServerInfo storedInfo = null;

    if(LOG.isDebugEnabled()) {
      LOG.debug("received start message from: " + server);
    }
    
    // If we get the startup message but there's an old server by that
    // name, then we can timeout the old one right away and register
    // the new one.
    storedInfo = serversToServerInfo.remove(server);

    if(storedInfo != null && !closed) {
      synchronized(msgQueue) {
        msgQueue.add(new PendingServerShutdown(storedInfo));
        msgQueue.notifyAll();
      }
    }

    // Either way, record the new server

    serversToServerInfo.put(server, serverInfo);

    if(!closed) {
      Text serverLabel = new Text(server);        
      serverLeases.createLease(serverLabel, serverLabel, new ServerExpirer(server));
    }
  }

  /** HRegionServers call this method repeatedly. */
  public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg msgs[]) throws IOException {
    String server = serverInfo.getServerAddress().toString().trim();
    Text serverLabel = new Text(server);

    if(closed
        || (msgs.length == 1 && msgs[0].getMsg() == HMsg.MSG_REPORT_EXITING)) {
      // We're shutting down. Or the HRegionServer is.
      serversToServerInfo.remove(server);
      serverLeases.cancelLease(serverLabel, serverLabel);
      HMsg returnMsgs[] = {new HMsg(HMsg.MSG_REGIONSERVER_STOP)};
      return returnMsgs;
    }

    HServerInfo storedInfo = serversToServerInfo.get(server);

    if(storedInfo == null) {

      if(LOG.isDebugEnabled()) {
        LOG.debug("received server report from unknown server: " + server);
      }

      // The HBaseMaster may have been restarted.
      // Tell the RegionServer to start over and call regionServerStartup()

      HMsg returnMsgs[] = new HMsg[1];
      returnMsgs[0] = new HMsg(HMsg.MSG_CALL_SERVER_STARTUP);
      return returnMsgs;

    } else if(storedInfo.getStartCode() != serverInfo.getStartCode()) {

      // This state is reachable if:
      //
      // 1) RegionServer A started
      // 2) RegionServer B started on the same machine, then 
      //    clobbered A in regionServerStartup.
      // 3) RegionServer A returns, expecting to work as usual.
      //
      // The answer is to ask A to shut down for good.

      if(LOG.isDebugEnabled()) {
        LOG.debug("region server race condition detected: " + server);
      }

      HMsg returnMsgs[] = {
          new HMsg(HMsg.MSG_REGIONSERVER_STOP)
      };
      return returnMsgs;

    } else {

      // All's well.  Renew the server's lease.
      // This will always succeed; otherwise, the fetch of serversToServerInfo
      // would have failed above.

      serverLeases.renewLease(serverLabel, serverLabel);

      // Refresh the info object
      serversToServerInfo.put(server, serverInfo);

      // Next, process messages for this server
      return processMsgs(serverInfo, msgs);
    }
  }

  /** Process all the incoming messages from a server that's contacted us. */
  HMsg[] processMsgs(HServerInfo info, HMsg incomingMsgs[]) throws IOException {
    Vector<HMsg> returnMsgs = new Vector<HMsg>();
    
    TreeMap<Text, HRegionInfo> regionsToKill =
      killList.remove(info.getServerAddress().toString());
    
    // Get reports on what the RegionServer did.
    
    for(int i = 0; i < incomingMsgs.length; i++) {
      HRegionInfo region = incomingMsgs[i].getRegionInfo();

      switch(incomingMsgs[i].getMsg()) {

      case HMsg.MSG_REPORT_OPEN:
        HRegionInfo regionInfo = unassignedRegions.get(region.regionName);

        if(regionInfo == null) {

          if(LOG.isDebugEnabled()) {
            LOG.debug("region server " + info.getServerAddress().toString()
                + "should not have opened region " + region.regionName);
          }

          // This Region should not have been opened.
          // Ask the server to shut it down, but don't report it as closed.  
          // Otherwise the HMaster will think the Region was closed on purpose, 
          // and then try to reopen it elsewhere; that's not what we want.

          returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE_WITHOUT_REPORT, region)); 

        } else {

          if(LOG.isDebugEnabled()) {
            LOG.debug(info.getServerAddress().toString() + " serving "
                + region.regionName);
          }

          // Remove from unassigned list so we don't assign it to someone else

          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          if(region.regionName.compareTo(HGlobals.rootRegionInfo.regionName) == 0) {

            // Store the Root Region location (in memory)

            rootRegionLocation = new HServerAddress(info.getServerAddress());

            // Wake up threads waiting for the root server

            rootRegionIsAvailable();
            break;

          } else if(region.regionName.find(META_TABLE_NAME.toString()) == 0) {

            // It's a meta region. Put it on the queue to be scanned.

            MetaRegion r = new MetaRegion();
            r.server = info.getServerAddress();
            r.regionName = region.regionName;
            r.startKey = region.startKey;

            synchronized(metaRegionsToScan) {
              metaRegionsToScan.add(r);
              metaRegionsToScan.notifyAll();
            }
          }

          // Queue up an update to note the region location.

          synchronized(msgQueue) {
            msgQueue.add(new PendingOpenReport(info, region.regionName));
            msgQueue.notifyAll();
          }
        }
        break;

      case HMsg.MSG_REPORT_CLOSE:
        if(LOG.isDebugEnabled()) {
          LOG.debug(info.getServerAddress().toString() + " no longer serving "
              + region.regionName);
        }

        if(region.regionName.compareTo(HGlobals.rootRegionInfo.regionName) == 0) { // Root region
          rootRegionLocation = null;
          unassignedRegions.put(region.regionName, region);
          assignAttempts.put(region.regionName, 0L);

        } else {
          boolean reassignRegion = true;
          boolean deleteRegion = false;

          if(killedRegions.remove(region.regionName)) {
            reassignRegion = false;
          }
            
          if(regionsToDelete.remove(region.regionName)) {
            reassignRegion = false;
            deleteRegion = true;
          }
          unassignedRegions.remove(region.regionName);
          assignAttempts.remove(region.regionName);

          synchronized(msgQueue) {
            msgQueue.add(new PendingCloseReport(region, reassignRegion, deleteRegion));
            msgQueue.notifyAll();
          }

          // NOTE: we cannot put the region into unassignedRegions as that
          //       could create a race with the pending close if it gets 
          //       reassigned before the close is processed.

        }
        break;

      case HMsg.MSG_NEW_REGION:
        if(LOG.isDebugEnabled()) {
          LOG.debug("new region " + region.regionName);
        }
        
        // A region has split and the old server is serving the two new regions.

        if(region.regionName.find(META_TABLE_NAME.toString()) == 0) {
          // A meta region has split.

          allMetaRegionsScanned = false;
        }
        
        break;

      default:
        throw new IOException("Impossible state during msg processing.  Instruction: "
            + incomingMsgs[i].getMsg());
      }
    }

    // Process the kill list
    
    if(regionsToKill != null) {
      for(HRegionInfo i: regionsToKill.values()) {
        returnMsgs.add(new HMsg(HMsg.MSG_REGION_CLOSE, i));
        killedRegions.add(i.regionName);
      }
    }

    // Figure out what the RegionServer ought to do, and write back.

    if(unassignedRegions.size() > 0) {

      // Open new regions as necessary

      int targetForServer = (int) Math.ceil(unassignedRegions.size()
          / (1.0 * serversToServerInfo.size()));

      int counter = 0;
      long now = System.currentTimeMillis();

      for(Iterator<Text> it = unassignedRegions.keySet().iterator();
          it.hasNext(); ) {

        Text curRegionName = it.next();
        HRegionInfo regionInfo = unassignedRegions.get(curRegionName);
        long assignedTime = assignAttempts.get(curRegionName);

        if(now - assignedTime > maxRegionOpenTime) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("assigning region " + regionInfo.regionName + " to server "
                + info.getServerAddress().toString());
          }

          returnMsgs.add(new HMsg(HMsg.MSG_REGION_OPEN, regionInfo));

          assignAttempts.put(curRegionName, now);
          counter++;
        }

        if(counter >= targetForServer) {
          break;
        }
      }
    }
    return returnMsgs.toArray(new HMsg[returnMsgs.size()]);
  }
  
  private synchronized void rootRegionIsAvailable() {
    notifyAll();
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Some internal classes to manage msg-passing and client operations
  //////////////////////////////////////////////////////////////////////////////
  
  private abstract class PendingOperation {
    protected final Text[] columns = {
        COLUMN_FAMILY
    };
    protected final Text startRow = new Text();
    protected long clientId;

    public PendingOperation() {
      this.clientId = rand.nextLong();
    }
    
    public abstract void process() throws IOException;
  }
  
  private class PendingServerShutdown extends PendingOperation {
    private String deadServer;
    private long oldStartCode;
    
    private class ToDoEntry {
      boolean deleteRegion;
      boolean regionOffline;
      HStoreKey key;
      HRegionInfo info;
      
      ToDoEntry(HStoreKey key, HRegionInfo info) {
        this.deleteRegion = false;
        this.regionOffline = false;
        this.key = key;
        this.info = info;
      }
    }
    
    public PendingServerShutdown(HServerInfo serverInfo) {
      super();
      this.deadServer = serverInfo.getServerAddress().toString();
      this.oldStartCode = serverInfo.getStartCode();
    }
    
    private void scanMetaRegion(HRegionInterface server, long scannerId,
        Text regionName) throws IOException {

      Vector<ToDoEntry> toDoList = new Vector<ToDoEntry>();
      TreeMap<Text, HRegionInfo> regions = new TreeMap<Text, HRegionInfo>();

      DataInputBuffer inbuf = new DataInputBuffer();
      try {
        while(true) {
          LabelledData[] values = null;
          
          HStoreKey key = new HStoreKey();
          try {
            values = server.next(scannerId, key);
            
          } catch(NotServingRegionException e) {
            throw e;
            
          } catch(IOException e) {
            LOG.error(e);
            break;
          }
          
          if(values == null || values.length == 0) {
            break;
          }

          TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
          for(int i = 0; i < values.length; i++) {
            byte[] bytes = new byte[values[i].getData().getSize()];
            System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
            results.put(values[i].getLabel(), bytes);
          }
          
          byte[] bytes = results.get(COL_SERVER); 
          String serverName = null;
          if(bytes == null || bytes.length == 0) {
            // No server
            continue;
          }
          try {
            serverName = new String(bytes, UTF8_ENCODING);
            
          } catch(UnsupportedEncodingException e) {
            LOG.error(e);
            break;
          }

          if(deadServer.compareTo(serverName) != 0) {
            // This isn't the server you're looking for - move along
            continue;
          }

          bytes = results.get(COL_STARTCODE);
          if(bytes == null || bytes.length == 0) {
            // No start code
            continue;
          }
          long startCode = -1L;
          
          try {
            startCode = Long.valueOf(new String(bytes, UTF8_ENCODING)).
              longValue();
          } catch(UnsupportedEncodingException e) {
            LOG.error(e);
            break;
          }

          if(oldStartCode != startCode) {
            // Close but no cigar
            continue;
          }

          // Bingo! Found it.

          bytes = results.get(COL_REGIONINFO);
          if(bytes == null || bytes.length == 0) {
            throw new IOException("no value for " + COL_REGIONINFO);
          }
          inbuf.reset(bytes, bytes.length);
          HRegionInfo info = new HRegionInfo();
          
          try {
            info.readFields(inbuf);
            
          } catch(IOException e) {
            LOG.error(e);
            break;
          }

          if(LOG.isDebugEnabled()) {
            LOG.debug(serverName + " was serving " + info.regionName);
          }

          ToDoEntry todo = new ToDoEntry(key, info);
          toDoList.add(todo);
          
          if(killList.containsKey(deadServer)) {
            TreeMap<Text, HRegionInfo> regionsToKill = killList.get(deadServer);
            if(regionsToKill.containsKey(info.regionName)) {
              regionsToKill.remove(info.regionName);
              killList.put(deadServer, regionsToKill);
              unassignedRegions.remove(info.regionName);
              assignAttempts.remove(info.regionName);
              
              if(regionsToDelete.contains(info.regionName)) {
                // Delete this region
                
                regionsToDelete.remove(info.regionName);
                todo.deleteRegion = true;
                
              } else {
                // Mark region offline
                
                todo.regionOffline = true;
              }
            }
          } else {
            // Get region reassigned

            regions.put(info.regionName, info);
          }
        }

      } finally {
        if(scannerId != -1L) {
          try {
            server.close(scannerId);
            
          } catch(IOException e) {
            LOG.error(e);
            
          }
        }
      }

      // Remove server from root/meta entries

      for(int i = 0; i < toDoList.size(); i++) {
        ToDoEntry e = toDoList.get(i);
        long lockid = server.startUpdate(regionName, clientId, e.key.getRow());
        if(e.deleteRegion) {
          server.delete(regionName, clientId, lockid, COL_REGIONINFO);
          
        } else if(e.regionOffline) {
          e.info.offLine = true;
          ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
          DataOutputStream s = new DataOutputStream(byteValue);
          e.info.write(s);

          server.put(regionName, clientId, lockid, COL_REGIONINFO,
              new BytesWritable(byteValue.toByteArray()));
        }
        server.delete(regionName, clientId, lockid, COL_SERVER);
        server.delete(regionName, clientId, lockid, COL_STARTCODE);
        server.commit(regionName, clientId, lockid);
      }

      // Get regions reassigned

      for(Map.Entry<Text, HRegionInfo> e: regions.entrySet()) {
        Text region = e.getKey();
        HRegionInfo regionInfo = e.getValue();

        unassignedRegions.put(region, regionInfo);
        assignAttempts.put(region, 0L);
      }
    }
    
    public void process() throws IOException {
      if(LOG.isDebugEnabled()) {
        LOG.debug("server shutdown: " + deadServer);
      }

      // Scan the ROOT region

      HRegionInterface server = null;
      long scannerId = -1L;
      for(int tries = 0; tries < numRetries; tries ++) {
        waitForRootRegion();      // Wait until the root region is available
        server = client.getHRegionConnection(rootRegionLocation);
        scannerId = -1L;
        
        try {
          scannerId = server.openScanner(HGlobals.rootRegionInfo.regionName, columns, startRow);
          scanMetaRegion(server, scannerId, HGlobals.rootRegionInfo.regionName);
          break;
          
        } catch(NotServingRegionException e) {
          if(tries == numRetries - 1) {
            throw e;
          }
        }
      }

      // We can not scan every meta region if they have not already been assigned
      // and scanned.

      for(int tries = 0; tries < numRetries; tries ++) {
        try {
          metaScanner.waitForMetaScan();
      
          for(Iterator<MetaRegion> i = knownMetaRegions.values().iterator();
              i.hasNext(); ) {
          
            server = null;
            scannerId = -1L;
            MetaRegion r = i.next();

            server = client.getHRegionConnection(r.server);
          
            scannerId = server.openScanner(r.regionName, columns, startRow);
            scanMetaRegion(server, scannerId, r.regionName);
            
          }
          break;
            
        } catch(NotServingRegionException e) {
          if(tries == numRetries - 1) {
            throw e;
          }
        }
      }
    }
  }
  
  /** PendingCloseReport is a close message that is saved in a different thread. */
  private class PendingCloseReport extends PendingOperation {
    private HRegionInfo regionInfo;
    private boolean reassignRegion;
    private boolean deleteRegion;
    private boolean rootRegion;
    
    public PendingCloseReport(HRegionInfo regionInfo, boolean reassignRegion,
        boolean deleteRegion) {
      
      super();

      this.regionInfo = regionInfo;
      this.reassignRegion = reassignRegion;
      this.deleteRegion = deleteRegion;

      // If the region closing down is a meta region then we need to update
      // the ROOT table
      
      if(this.regionInfo.regionName.find(HGlobals.metaTableDesc.getName().toString()) == 0) {
        this.rootRegion = true;
        
      } else {
        this.rootRegion = false;
      }
    }
    
    public void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries ++) {

        // We can not access any meta region if they have not already been assigned
        // and scanned.

        metaScanner.waitForMetaScan();

        if(LOG.isDebugEnabled()) {
          LOG.debug("region closed: " + regionInfo.regionName);
        }

        // Mark the Region as unavailable in the appropriate meta table

        Text metaRegionName;
        HRegionInterface server;
        if (rootRegion) {
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          waitForRootRegion();            // Make sure root region available
          server = client.getHRegionConnection(rootRegionLocation);

        } else {
          MetaRegion r = null;
          if(knownMetaRegions.containsKey(regionInfo.regionName)) {
            r = knownMetaRegions.get(regionInfo.regionName);

          } else {
            r = knownMetaRegions.get(
                knownMetaRegions.headMap(regionInfo.regionName).lastKey());
          }
          metaRegionName = r.regionName;
          server = client.getHRegionConnection(r.server);
        }

        try {
          long lockid = server.startUpdate(metaRegionName, clientId, regionInfo.regionName);
          if(deleteRegion) {
            server.delete(metaRegionName, clientId, lockid, COL_REGIONINFO);
            
          } else if(!reassignRegion ) {
            regionInfo.offLine = true;
            ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
            DataOutputStream s = new DataOutputStream(byteValue);
            regionInfo.write(s);

            server.put(metaRegionName, clientId, lockid, COL_REGIONINFO,
                new BytesWritable(byteValue.toByteArray()));
          }
          server.delete(metaRegionName, clientId, lockid, COL_SERVER);
          server.delete(metaRegionName, clientId, lockid, COL_STARTCODE);
          server.commit(metaRegionName, clientId, lockid);
          break;

        } catch(NotServingRegionException e) {
          if(tries == numRetries - 1) {
            throw e;
          }
          continue;
        }
      }

      if(reassignRegion) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("reassign region: " + regionInfo.regionName);
        }
        
        unassignedRegions.put(regionInfo.regionName, regionInfo);
        assignAttempts.put(regionInfo.regionName, 0L);
        
      } else if(deleteRegion) {
        try {
          HRegion.deleteRegion(fs, dir, regionInfo.regionName);

        } catch(IOException e) {
          LOG.error("failed to delete region " + regionInfo.regionName);
          LOG.error(e);
          throw e;
        }
      }
    }
  }

  /** PendingOpenReport is an open message that is saved in a different thread. */
  private class PendingOpenReport extends PendingOperation {
    private boolean rootRegion;
    private Text regionName;
    private BytesWritable serverAddress;
    private BytesWritable startCode;
    
    public PendingOpenReport(HServerInfo info, Text regionName) {
      if(regionName.find(HGlobals.metaTableDesc.getName().toString()) == 0) {
        
        // The region which just came on-line is a META region.
        // We need to look in the ROOT region for its information.
        
        this.rootRegion = true;
        
      } else {
        
        // Just an ordinary region. Look for it in the META table.
        
        this.rootRegion = false;
      }
      this.regionName = regionName;
      
      try {
        this.serverAddress = new BytesWritable(
            info.getServerAddress().toString().getBytes(UTF8_ENCODING));
        
        this.startCode = new BytesWritable(
            String.valueOf(info.getStartCode()).getBytes(UTF8_ENCODING));
        
      } catch(UnsupportedEncodingException e) {
        LOG.error(e);
      }

    }
    
    public void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries ++) {

        // We can not access any meta region if they have not already been assigned
        // and scanned.

        metaScanner.waitForMetaScan();

        if(LOG.isDebugEnabled()) {
          LOG.debug(regionName + " open on "
              + new String(serverAddress.get(), UTF8_ENCODING));
        }

        // Register the newly-available Region's location.

        Text metaRegionName;
        HRegionInterface server;
        if(rootRegion) {
          metaRegionName = HGlobals.rootRegionInfo.regionName;
          waitForRootRegion();            // Make sure root region available
          server = client.getHRegionConnection(rootRegionLocation);

        } else {
          MetaRegion r = null;
          if(knownMetaRegions.containsKey(regionName)) {
            r = knownMetaRegions.get(regionName);

          } else {
            r = knownMetaRegions.get(
                knownMetaRegions.headMap(regionName).lastKey());
          }
          metaRegionName = r.regionName;
          server = client.getHRegionConnection(r.server);
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("updating row " + regionName + " in table " + metaRegionName);
        }
        try {
          long lockid = server.startUpdate(metaRegionName, clientId, regionName);
          server.put(metaRegionName, clientId, lockid, COL_SERVER, serverAddress);
          server.put(metaRegionName, clientId, lockid, COL_STARTCODE, startCode);
          server.commit(metaRegionName, clientId, lockid);
          break;
          
        } catch(NotServingRegionException e) {
          if(tries == numRetries - 1) {
            throw e;
          }
        }
      }
    }
  }

  synchronized void waitForRootRegion() {
    while (rootRegionLocation == null) {
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wait for root region");
        }
        wait();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wake from wait for root region");
        }
      } catch(InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wake from wait for root region (IE)");
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMasterInterface
  //////////////////////////////////////////////////////////////////////////////
  
  public boolean isMasterRunning() {
    return !closed;
  }

  public void createTable(HTableDescriptor desc) throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo newRegion = new HRegionInfo(rand.nextLong(), desc, null, null);

    for(int tries = 0; tries < numRetries; tries++) {
      try {
        // We can not access any meta region if they have not already been assigned
        // and scanned.

        metaScanner.waitForMetaScan();

        // 1. Check to see if table already exists

        MetaRegion m = null;
        if(knownMetaRegions.containsKey(newRegion.regionName)) {
          m = knownMetaRegions.get(newRegion.regionName);

        } else {
          m = knownMetaRegions.get(
              knownMetaRegions.headMap(newRegion.regionName).lastKey());
        }
        Text metaRegionName = m.regionName;
        HRegionInterface server = client.getHRegionConnection(m.server);


        BytesWritable bytes = server.get(metaRegionName, desc.getName(), COL_REGIONINFO);
        if(bytes != null && bytes.getSize() != 0) {
          byte[] infoBytes = bytes.get();
          DataInputBuffer inbuf = new DataInputBuffer();
          inbuf.reset(infoBytes, infoBytes.length);
          HRegionInfo info = new HRegionInfo();
          info.readFields(inbuf);
          if(info.tableDesc.getName().compareTo(desc.getName()) == 0) {
            throw new IOException("table already exists");
          }
        }

        // 2. Create the HRegion

        HRegion r = HRegion.createHRegion(newRegion.regionId, desc, this.dir,
          this.conf);

        // 3. Insert into meta

        HRegionInfo info = r.getRegionInfo();
        Text regionName = r.getRegionName();
        ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(byteValue);
        info.write(s);

        long clientId = rand.nextLong();
        long lockid = server.startUpdate(metaRegionName, clientId, regionName);
        server.put(metaRegionName, clientId, lockid, COL_REGIONINFO, 
            new BytesWritable(byteValue.toByteArray()));
        server.commit(metaRegionName, clientId, lockid);

        // 4. Close the new region to flush it to disk

        r.close();

        // 5. Get it assigned to a server

        unassignedRegions.put(regionName, info);
        assignAttempts.put(regionName, 0L);
        break;

      } catch(NotServingRegionException e) {
        if(tries == numRetries - 1) {
          throw e;
        }
      }
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("created table " + desc.getName());
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#deleteTable(org.apache.hadoop.io.Text)
   */
  public void deleteTable(Text tableName) throws IOException {
    new TableDelete(tableName).process();
    if(LOG.isDebugEnabled()) {
      LOG.debug("deleted table: " + tableName);
    }
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#addColumn(org.apache.hadoop.io.Text, org.apache.hadoop.hbase.HColumnDescriptor)
   */
  public void addColumn(Text tableName, HColumnDescriptor column) throws IOException {
    new AddColumn(tableName, column).process();
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#deleteColumn(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text)
   */
  public void deleteColumn(Text tableName, Text columnName) throws IOException {
    new DeleteColumn(tableName, HStoreKey.extractFamily(columnName)).process();
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#enableTable(org.apache.hadoop.io.Text)
   */
  public void enableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, true).process();
  }
  
  /** 
   * Turn off the HMaster.  Sets a flag so that the main thread know to shut
   * things down in an orderly fashion.
   */
  public void shutdown() {
    TimerTask tt = new TimerTask() {
      @Override
      public void run() {
        closed = true;
        synchronized(msgQueue) {
          msgQueue.clear();                         // Empty the queue
          msgQueue.notifyAll();                     // Wake main thread
        }
      }
    };
    Timer t = new Timer("Shutdown");
    t.schedule(tt, 10);
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#findRootRegion()
   */
  public HServerAddress findRootRegion() {
    return rootRegionLocation;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.HMasterInterface#disableTable(org.apache.hadoop.io.Text)
   */
  public void disableTable(Text tableName) throws IOException {
    new ChangeTableState(tableName, false).process();
  }
  
  // Helper classes for HMasterInterface

  private abstract class TableOperation {
    private SortedSet<MetaRegion> metaRegions;
    protected Text tableName;
    
    protected TreeSet<HRegionInfo> unservedRegions;
    
    protected TableOperation(Text tableName) throws IOException {
      if (!isMasterRunning()) {
        throw new MasterNotRunningException();
      }
      this.metaRegions = new TreeSet<MetaRegion>();
      this.tableName = tableName;
      this.unservedRegions = new TreeSet<HRegionInfo>();

      // We can not access any meta region if they have not already been
      // assigned and scanned.

      metaScanner.waitForMetaScan();

      Text firstMetaRegion = null;
      if(knownMetaRegions.size() == 1) {
        firstMetaRegion = knownMetaRegions.firstKey();

      } else if(knownMetaRegions.containsKey(tableName)) {
        firstMetaRegion = tableName;

      } else {
        firstMetaRegion = knownMetaRegions.headMap(tableName).lastKey();
      }

      this.metaRegions.addAll(knownMetaRegions.tailMap(firstMetaRegion).values());
    }
    
    public void process() throws IOException {
      for(int tries = 0; tries < numRetries; tries++) {
        boolean tableExists = false;
        try {
          synchronized(metaScannerLock) {     // Prevent meta scanner from running
            for(MetaRegion m: metaRegions) {

              // Get a connection to a meta server

              HRegionInterface server = client.getHRegionConnection(m.server);

              // Open a scanner on the meta region
              
              long scannerId =
                server.openScanner(m.regionName, METACOLUMNS, tableName);
              
              try {
                DataInputBuffer inbuf = new DataInputBuffer();
                byte[] bytes;
                while(true) {
                  HRegionInfo info = new HRegionInfo();
                  String serverName = null;
                  long startCode = -1L;
                  
                  LabelledData[] values = null;
                  HStoreKey key = new HStoreKey();
                  values = server.next(scannerId, key);
                  if(values == null || values.length == 0) {
                    break;
                  }
                  boolean haveRegionInfo = false;
                  for(int i = 0; i < values.length; i++) {
                    bytes = new byte[values[i].getData().getSize()];
                    if(bytes.length == 0) {
                      break;
                    }
                    System.arraycopy(values[i].getData().get(), 0, bytes, 0, bytes.length);
                   
                    if(values[i].getLabel().equals(COL_REGIONINFO)) {
                      haveRegionInfo = true;
                      inbuf.reset(bytes, bytes.length);
                      info.readFields(inbuf);
                      
                    } else if(values[i].getLabel().equals(COL_SERVER)) {
                      try {
                        serverName = new String(bytes, UTF8_ENCODING);
                        
                      } catch(UnsupportedEncodingException e) {
                        assert(false);
                      }
                      
                    } else if(values[i].getLabel().equals(COL_STARTCODE)) {
                      try {
                        startCode = Long.valueOf(new String(bytes, UTF8_ENCODING));
                        
                      } catch(UnsupportedEncodingException e) {
                        assert(false);
                      }
                    }
                  }
                  
                  if(!haveRegionInfo) {
                    throw new IOException(COL_REGIONINFO + " not found");
                  }
                  
                  if(info.tableDesc.getName().compareTo(tableName) > 0) {
                    break;               // Beyond any more entries for this table
                  }
                  
                  tableExists = true;
                  if(!isBeingServed(serverName, startCode)) {
                    unservedRegions.add(info);
                  }
                  processScanItem(serverName, startCode, info);

                } // while(true)
                
              } finally {
                if(scannerId != -1L) {
                  try {
                    server.close(scannerId);

                  } catch(IOException e) {
                    LOG.error(e);
                  }
                }
                scannerId = -1L;
              }
              
              if(!tableExists) {
                throw new IOException(tableName + " does not exist");
              }
              
              postProcessMeta(m, server);
              unservedRegions.clear();
              
            } // for(MetaRegion m:)
          } // synchronized(metaScannerLock)
          
        } catch(NotServingRegionException e) {
          if(tries == numRetries - 1) {
            throw e;
          }
          continue;
        }
        break;
      } // for(tries...)
    }
    
    protected boolean isBeingServed(String serverName, long startCode) {
      boolean result = false;
      if(serverName != null && startCode != -1L) {
        HServerInfo s = serversToServerInfo.get(serverName);
        result = s != null && s.getStartCode() == startCode;
      }
      return result;
    }
    
    protected boolean isEnabled(HRegionInfo info) {
      return !info.offLine;
    }
    
    protected abstract void processScanItem(String serverName, long startCode,
        HRegionInfo info) throws IOException;
    
    protected abstract void postProcessMeta(MetaRegion m,
        HRegionInterface server)
    throws IOException;
  }

  private class ChangeTableState extends TableOperation {
    private boolean online;
    
    protected TreeMap<String, TreeSet<HRegionInfo>> servedRegions =
      new TreeMap<String, TreeSet<HRegionInfo>>();
    protected long lockid;
    protected long clientId;
    
    public ChangeTableState(Text tableName, boolean onLine) throws IOException {
      super(tableName);
      this.online = onLine;
    }
    
    protected void processScanItem(String serverName, long startCode,
        HRegionInfo info)
    throws IOException {
      if (isBeingServed(serverName, startCode)) {
        TreeSet<HRegionInfo> regions = servedRegions.get(serverName);
        if (regions == null) {
          regions = new TreeSet<HRegionInfo>();
        }
        regions.add(info);
        servedRegions.put(serverName, regions);
      }
    }
    
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {
      // Process regions not being served
      if(LOG.isDebugEnabled()) {
        LOG.debug("processing unserved regions");
      }
      for(HRegionInfo i: unservedRegions) {
        // Update meta table
        if(LOG.isDebugEnabled()) {
          LOG.debug("updating columns in row: " + i.regionName);
        }

        lockid = -1L;
        clientId = rand.nextLong();
        try {
          lockid = server.startUpdate(m.regionName, clientId, i.regionName);
          updateRegionInfo(server, m.regionName, i);
          server.delete(m.regionName, clientId, lockid, COL_SERVER);
          server.delete(m.regionName, clientId, lockid, COL_STARTCODE);
          server.commit(m.regionName, clientId, lockid);
          lockid = -1L;

          if(LOG.isDebugEnabled()) {
            LOG.debug("updated columns in row: " + i.regionName);
          }

        } catch(NotServingRegionException e) {
          throw e;

        } catch(IOException e) {
          LOG.error("column update failed in row: " + i.regionName);
          LOG.error(e);

        } finally {
          try {
            if(lockid != -1L) {
              server.abort(m.regionName, clientId, lockid);
            }

          } catch(IOException iex) {
            LOG.error(iex);
          }
        }

        if(online) {                            // Bring offline regions on-line
          if(!unassignedRegions.containsKey(i.regionName)) {
            unassignedRegions.put(i.regionName, i);
            assignAttempts.put(i.regionName, 0L);
          }
          
        } else {                                // Prevent region from getting assigned.
          unassignedRegions.remove(i.regionName);
          assignAttempts.remove(i.regionName);
        }
      }
      
      // Process regions currently being served
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("processing regions currently being served");
      }
      for(Map.Entry<String, TreeSet<HRegionInfo>> e: servedRegions.entrySet()) {
        String serverName = e.getKey();
        if (online) {
          LOG.debug("Already online");
          continue;                             // Already being served
        }
        
        // Cause regions being served to be taken off-line and disabled
        TreeMap<Text, HRegionInfo> localKillList = killList.get(serverName);
        if(localKillList == null) {
          localKillList = new TreeMap<Text, HRegionInfo>();
        }
        for(HRegionInfo i: e.getValue()) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("adding region " + i.regionName + " to local kill list");
          }
          localKillList.put(i.regionName, i);
        }
        if(localKillList.size() > 0) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("inserted local kill list into kill list for server " +
              serverName);
          }
          killList.put(serverName, localKillList);
        }
      }
      servedRegions.clear();
    }
    
    protected void updateRegionInfo(HRegionInterface server, Text regionName,
        HRegionInfo i) throws IOException {
      
      i.offLine = !online;
      
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      i.write(s);

      server.put(regionName, clientId, lockid, COL_REGIONINFO,
          new BytesWritable(byteValue.toByteArray()));
      
    }
  }
  
  private class TableDelete extends ChangeTableState {
    
    public TableDelete(Text tableName) throws IOException {
      super(tableName, false);
    }
    
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {
      // For regions that are being served, mark them for deletion      
      for (TreeSet<HRegionInfo> s: servedRegions.values()) {
        for (HRegionInfo i: s) {
          regionsToDelete.add(i.regionName);
        }
      }

      // Unserved regions we can delete now
      for (HRegionInfo i: unservedRegions) {
        // Delete the region
        try {
          HRegion.deleteRegion(fs, dir, i.regionName);
        } catch(IOException e) {
          LOG.error("failed to delete region " + i.regionName);
          LOG.error(e);
        }
      }
      super.postProcessMeta(m, server);
    }
    
    @Override
    protected void updateRegionInfo(
      @SuppressWarnings("hiding") HRegionInterface server, Text regionName,
      @SuppressWarnings("unused") HRegionInfo i)
    throws IOException {
      server.delete(regionName, clientId, lockid, COL_REGIONINFO);
    }
  }
  
  private abstract class ColumnOperation extends TableOperation {
    protected ColumnOperation(Text tableName) throws IOException {
      super(tableName);
    }

    protected void processScanItem(
      @SuppressWarnings("unused") String serverName,
      @SuppressWarnings("unused") long startCode, final HRegionInfo info)
    throws IOException {  
      if(isEnabled(info)) {
        throw new TableNotDisabledException(tableName.toString());
      }
    }

    protected void updateRegionInfo(HRegionInterface server, Text regionName,
        HRegionInfo i) throws IOException {
      
      ByteArrayOutputStream byteValue = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteValue);
      i.write(s);

      long lockid = -1L;
      long clientId = rand.nextLong();
      try {
        lockid = server.startUpdate(regionName, clientId, i.regionName);
        server.put(regionName, clientId, lockid, COL_REGIONINFO,
            new BytesWritable(byteValue.toByteArray()));
      
        server.commit(regionName, clientId, lockid);
        lockid = -1L;

        if(LOG.isDebugEnabled()) {
          LOG.debug("updated columns in row: " + i.regionName);
        }

      } catch(NotServingRegionException e) {
        throw e;

      } catch(IOException e) {
        LOG.error("column update failed in row: " + i.regionName);
        LOG.error(e);

      } finally {
        if(lockid != -1L) {
          try {
            server.abort(regionName, clientId, lockid);
            
          } catch(IOException iex) {
            LOG.error(iex);
          }
        }
      }
    }
  }
  
  private class DeleteColumn extends ColumnOperation {
    private Text columnName;
    
    public DeleteColumn(Text tableName, Text columnName) throws IOException {
      super(tableName);
      this.columnName = columnName;
    }
    
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {

      for(HRegionInfo i: unservedRegions) {
        i.tableDesc.families().remove(columnName);
        updateRegionInfo(server, m.regionName, i);
        
        // Delete the directories used by the column

        try {
          fs.delete(HStoreFile.getMapDir(dir, i.regionName, columnName));
          
        } catch(IOException e) {
          LOG.error(e);
        }
        
        try {
          fs.delete(HStoreFile.getInfoDir(dir, i.regionName, columnName));
          
        } catch(IOException e) {
          LOG.error(e);
        }
        
      }
    }
  }
  
  private class AddColumn extends ColumnOperation {
    private HColumnDescriptor newColumn;
    
    public AddColumn(Text tableName, HColumnDescriptor newColumn)
        throws IOException {
      
      super(tableName);
      this.newColumn = newColumn;
    }
   
    protected void postProcessMeta(MetaRegion m, HRegionInterface server)
        throws IOException {

      for(HRegionInfo i: unservedRegions) {
        
        //TODO: I *think* all we need to do to add a column is add it to
        // the table descriptor. When the region is brought on-line, it
        // should find the column missing and create it.
        
        i.tableDesc.addFamily(newColumn);
        updateRegionInfo(server, m.regionName, i);
      }
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Managing leases
  //////////////////////////////////////////////////////////////////////////////
  
  private class ServerExpirer extends LeaseListener {
    private String server;
    
    public ServerExpirer(String server) {
      this.server = server;
    }
    
    public void leaseExpired() {
      LOG.info(server + " lease expired");
      HServerInfo storedInfo = serversToServerInfo.remove(server);
      synchronized(msgQueue) {
        msgQueue.add(new PendingServerShutdown(storedInfo));
        msgQueue.notifyAll();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Main program
  //////////////////////////////////////////////////////////////////////////////
  
  private static void printUsageAndExit() {
    System.err.println("Usage: java org.apache.hbase.HMaster " +
        "[--bind=hostname:port] start|stop");
    System.exit(0);
  }
  
  public static void main(String [] args) {
    if (args.length < 1) {
      printUsageAndExit();
    }
    
    Configuration conf = new HBaseConfiguration();
    
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
          (new Thread(new HMaster(conf))).start();
        } catch (Throwable t) {
          LOG.error( "Can not start master because "+
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
        break;
      }
      
      if (cmd.equals("stop")) {
        try {
          HClient client = new HClient(conf);
          client.shutdown();
        } catch (Throwable t) {
          LOG.error( "Can not stop master because " +
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
        break;
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
  }
}
