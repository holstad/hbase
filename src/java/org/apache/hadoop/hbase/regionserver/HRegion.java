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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/**
 * HRegion stores data for a certain region of a table.  It stores all columns
 * for each row. A given table consists of one or more HRegions.
 *
 * <p>We maintain multiple HStores for a single HRegion.
 * 
 * <p>An HStore is a set of rows with some column data; together,
 * they make up all the data for the rows.  
 *
 * <p>Each HRegion has a 'startKey' and 'endKey'.
 * <p>The first is inclusive, the second is exclusive (except for
 * the final region)  The endKey of region 0 is the same as
 * startKey for region 1 (if it exists).  The startKey for the
 * first region is null. The endKey for the final region is null.
 *
 * <p>Locking at the HRegion level serves only one purpose: preventing the
 * region from being closed (and consequently split) while other operations
 * are ongoing. Each row level operation obtains both a row lock and a region
 * read lock for the duration of the operation. While a scanner is being
 * constructed, getScanner holds a read lock. If the scanner is successfully
 * constructed, it holds a read lock until it is closed. A close takes out a
 * write lock and consequently will block for ongoing operations and will block
 * new operations from starting while the close is in progress.
 * 
 * <p>An HRegion is defined by its table and its key extent.
 * 
 * <p>It consists of at least one HStore.  The number of HStores should be 
 * configurable, so that data which is accessed together is stored in the same
 * HStore.  Right now, we approximate that by building a single HStore for 
 * each column family.  (This config info will be communicated via the 
 * tabledesc.)
 * 
 * <p>The HTableDescriptor contains metainfo about the HRegion's table.
 * regionName is a unique identifier for this HRegion. (startKey, endKey]
 * defines the keyspace for this HRegion.
 */
public class HRegion implements HConstants {
  static final String SPLITDIR = "splits";
  static final String MERGEDIR = "merges";
  static final Random rand = new Random();
  static final Log LOG = LogFactory.getLog(HRegion.class);
  final AtomicBoolean closed = new AtomicBoolean(false);
  private final RegionHistorian historian;

  /**
   * Merge two HRegions.  The regions must be adjacent andmust not overlap.
   * 
   * @param srcA
   * @param srcB
   * @return new merged HRegion
   * @throws IOException
   */
  public static HRegion mergeAdjacent(final HRegion srcA, final HRegion srcB)
  throws IOException {

    HRegion a = srcA;
    HRegion b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    if (srcA.getStartKey() == null) {
      if (srcB.getStartKey() == null) {
        throw new IOException("Cannot merge two regions with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getStartKey() == null)         // A is not null but B is
        || (Bytes.compareTo(srcA.getStartKey(), srcB.getStartKey()) > 0)) { // A > B
      a = srcB;
      b = srcA;
    }

    if (!Bytes.equals(a.getEndKey(), b.getStartKey())) {
      throw new IOException("Cannot merge non-adjacent regions");
    }
    return merge(a, b);
  }

  /**
   * Merge two regions whether they are adjacent or not.
   * 
   * @param a region a
   * @param b region b
   * @return new merged region
   * @throws IOException
   */
  public static HRegion merge(HRegion a, HRegion b) throws IOException {
    if (!a.getRegionInfo().getTableDesc().getNameAsString().equals(
        b.getRegionInfo().getTableDesc().getNameAsString())) {
      throw new IOException("Regions do not belong to the same table");
    }

    FileSystem fs = a.getFilesystem();

    // Make sure each region's cache is empty
    
    a.flushcache();
    b.flushcache();
    
    // Compact each region so we only have one store file per family
    
    a.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + a);
      listPaths(fs, a.getRegionDir());
    }
    b.compactStores(true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for region: " + b);
      listPaths(fs, b.getRegionDir());
    }
    
    HBaseConfiguration conf = a.getConf();
    HTableDescriptor tabledesc = a.getTableDesc();
    HLog log = a.getLog();
    Path basedir = a.getBaseDir();
    final byte [] startKey = Bytes.equals(a.getStartKey(), EMPTY_BYTE_ARRAY) ||
      Bytes.equals(b.getStartKey(), EMPTY_BYTE_ARRAY) ? EMPTY_BYTE_ARRAY :
        Bytes.compareTo(a.getStartKey(), b.getStartKey()) <= 0 ?
            a.getStartKey() : b.getStartKey();
    final byte [] endKey = Bytes.equals(a.getEndKey(), EMPTY_BYTE_ARRAY) ||
      Bytes.equals(b.getEndKey(), EMPTY_BYTE_ARRAY) ? EMPTY_BYTE_ARRAY :
        Bytes.compareTo(a.getEndKey(), b.getEndKey()) <= 0 ?
            b.getEndKey() : a.getEndKey();

    HRegionInfo newRegionInfo = new HRegionInfo(tabledesc, startKey, endKey);
    LOG.info("Creating new region " + newRegionInfo.toString());
    int encodedName = newRegionInfo.getEncodedName(); 
    Path newRegionDir = HRegion.getRegionDir(a.getBaseDir(), encodedName);
    if(fs.exists(newRegionDir)) {
      throw new IOException("Cannot merge; target file collision at " +
          newRegionDir);
    }
    fs.mkdirs(newRegionDir);

    LOG.info("starting merge of regions: " + a + " and " + b +
      " into new region " + newRegionInfo.toString() +
        " with start key <" + startKey + "> and end key <" + endKey + ">");

    // Move HStoreFiles under new region directory
    
    Map<byte [], List<HStoreFile>> byFamily =
      new TreeMap<byte [], List<HStoreFile>>(Bytes.BYTES_COMPARATOR);
    byFamily = filesByFamily(byFamily, a.close());
    byFamily = filesByFamily(byFamily, b.close());
    for (Map.Entry<byte [], List<HStoreFile>> es : byFamily.entrySet()) {
      byte [] colFamily = es.getKey();
      makeColumnFamilyDirs(fs, basedir, encodedName, colFamily, tabledesc);
      
      // Because we compacted the source regions we should have no more than two
      // HStoreFiles per family and there will be no reference store
      List<HStoreFile> srcFiles = es.getValue();
      if (srcFiles.size() == 2) {
        long seqA = srcFiles.get(0).loadInfo(fs);
        long seqB = srcFiles.get(1).loadInfo(fs);
        if (seqA == seqB) {
          // We can't have duplicate sequence numbers
          if (LOG.isDebugEnabled()) {
            LOG.debug("Adjusting sequence id of storeFile " + srcFiles.get(1) +
              " down by one; sequence id A=" + seqA + ", sequence id B=" +
              seqB);
          }
          srcFiles.get(1).writeInfo(fs, seqB - 1);
        }
      }
      for (HStoreFile hsf: srcFiles) {
        HStoreFile dst = new HStoreFile(conf, fs, basedir,
            newRegionInfo.getEncodedName(), colFamily, -1, null);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renaming " + hsf + " to " + dst);
        }
        hsf.rename(fs, dst);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, newRegionDir);
    }
    HRegion dstRegion = new HRegion(basedir, log, fs, conf, newRegionInfo,
        null, null);
    dstRegion.compactStores();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Files for new region");
      listPaths(fs, dstRegion.getRegionDir());
    }
    deleteRegion(fs, a.getRegionDir());
    deleteRegion(fs, b.getRegionDir());

    LOG.info("merge completed. New region is " + dstRegion);

    return dstRegion;
  }

  /*
   * Fills a map with a vector of store files keyed by column family. 
   * @param byFamily Map to fill.
   * @param storeFiles Store files to process.
   * @return Returns <code>byFamily</code>
   */
  private static Map<byte [], List<HStoreFile>> filesByFamily(
      Map<byte [], List<HStoreFile>> byFamily, List<HStoreFile> storeFiles) {
    for (HStoreFile src: storeFiles) {
      List<HStoreFile> v = byFamily.get(src.getColFamily());
      if (v == null) {
        v = new ArrayList<HStoreFile>();
        byFamily.put(src.getColFamily(), v);
      }
      v.add(src);
    }
    return byFamily;
  }

  /*
   * Method to list files in use by region
   */
  static void listFiles(FileSystem fs, HRegion r) throws IOException {
    listPaths(fs, r.getRegionDir());
  }
  
  /*
   * List the files under the specified directory
   * 
   * @param fs
   * @param dir
   * @throws IOException
   */
  private static void listPaths(FileSystem fs, Path dir) throws IOException {
    if (LOG.isDebugEnabled()) {
      FileStatus[] stats = fs.listStatus(dir);
      if (stats == null || stats.length == 0) {
        return;
      }
      for (int i = 0; i < stats.length; i++) {
        String path = stats[i].getPath().toString();
        if (stats[i].isDir()) {
          LOG.debug("d " + path);
          listPaths(fs, stats[i].getPath());
        } else {
          LOG.debug("f " + path + " size=" + stats[i].getLen());
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Members
  //////////////////////////////////////////////////////////////////////////////

  private final Map<Integer, byte []> locksToRows =
    new ConcurrentHashMap<Integer, byte []>();
  private final Map<Integer, TreeMap<HStoreKey, byte []>> targetColumns =
      new ConcurrentHashMap<Integer, TreeMap<HStoreKey, byte []>>();
  // Default access because read by tests.
  final Map<Integer, HStore> stores = new ConcurrentHashMap<Integer, HStore>();
  final AtomicLong memcacheSize = new AtomicLong(0);

  final Path basedir;
  final HLog log;
  final FileSystem fs;
  final HBaseConfiguration conf;
  final HRegionInfo regionInfo;
  final Path regiondir;
  private final Path regionCompactionDir;

  /*
   * Data structure of write state flags used coordinating flushes,
   * compactions and closes.
   */
  static class WriteState {
    // Set while a memcache flush is happening.
    volatile boolean flushing = false;
    // Set when a flush has been requested.
    volatile boolean flushRequested = false;
    // Set while a compaction is running.
    volatile boolean compacting = false;
    // Gets set in close. If set, cannot compact or flush again.
    volatile boolean writesEnabled = true;
    // Set if region is read-only
    private volatile boolean readOnly = false;
    
    /**
     * Set flags that make this region read-only.
     */
    synchronized void setReadOnly(final boolean onOff) {
      this.writesEnabled = !onOff;
      this.readOnly = onOff;
    }
    
    boolean isReadOnly() {
      return this.readOnly;
    }

    boolean isFlushRequested() {
      return this.flushRequested;
    }
  }

  private volatile WriteState writestate = new WriteState();

  final int memcacheFlushSize;
  private volatile long lastFlushTime;
  final FlushRequester flushListener;
  private final int blockingMemcacheSize;
  final long threadWakeFrequency;
  // Used to guard splits and closes
  private final ReentrantReadWriteLock splitsAndClosesLock =
    new ReentrantReadWriteLock();
  // Stop updates lock
  private final ReentrantReadWriteLock updatesLock =
    new ReentrantReadWriteLock();
  private final Integer splitLock = new Integer(0);
  private final long minSequenceId;
  final AtomicInteger activeScannerCount = new AtomicInteger(0);

  //////////////////////////////////////////////////////////////////////////////
  // Constructor
  //////////////////////////////////////////////////////////////////////////////

  /**
   * HRegion constructor.
   *
   * @param basedir qualified path of directory where region should be located,
   * usually the table directory.
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * @param initialFiles If there are initial files (implying that the HRegion
   * is new), then read them from the supplied path.
   * @param flushListener an object that implements CacheFlushListener or null
   * or null
   * @throws IOException
   */
  public HRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration conf, 
      HRegionInfo regionInfo, Path initialFiles,
      FlushRequester flushListener) throws IOException {
    this(basedir, log, fs, conf, regionInfo, initialFiles, flushListener, null);
  }
  
  /**
   * HRegion constructor.
   *
   * @param log The HLog is the outbound log for any updates to the HRegion
   * (There's a single HLog for all the HRegions on a single HRegionServer.)
   * The log file is a logfile from the previous execution that's
   * custom-computed for this HRegion. The HRegionServer computes and sorts the
   * appropriate log info for this HRegion. If there is a previous log file
   * (implying that the HRegion has been written-to before), then read it from
   * the supplied path.
   * @param basedir qualified path of directory where region should be located,
   * usually the table directory.
   * @param fs is the filesystem.  
   * @param conf is global configuration settings.
   * @param regionInfo - HRegionInfo that describes the region
   * @param initialFiles If there are initial files (implying that the HRegion
   * is new), then read them from the supplied path.
   * @param flushListener an object that implements CacheFlushListener or null
   * @param reporter Call on a period so hosting server can report we're
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   * @throws IOException
   */
  public HRegion(Path basedir, HLog log, FileSystem fs, HBaseConfiguration conf, 
      HRegionInfo regionInfo, Path initialFiles,
      FlushRequester flushListener, final Progressable reporter)
    throws IOException {
    
    this.basedir = basedir;
    this.log = log;
    this.fs = fs;
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.flushListener = flushListener;
    this.threadWakeFrequency = conf.getLong(THREAD_WAKE_FREQUENCY, 10 * 1000);
    String encodedNameStr = Integer.toString(this.regionInfo.getEncodedName());
    this.regiondir = new Path(basedir, encodedNameStr);
    Path oldLogFile = new Path(regiondir, HREGION_OLDLOGFILE_NAME);
    this.historian = RegionHistorian.getInstance();
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region " + this + "/" +
        this.regionInfo.getEncodedName());
    }
    
    this.regionCompactionDir =
      new Path(getCompactionDir(basedir), encodedNameStr);

    // Move prefab HStore files into place (if any).  This picks up split files
    // and any merges from splits and merges dirs.
    if (initialFiles != null && fs.exists(initialFiles)) {
      fs.rename(initialFiles, this.regiondir);
    }

    // Load in all the HStores.
    long maxSeqId = -1;
    for (HColumnDescriptor c : this.regionInfo.getTableDesc().getFamilies()) {
      HStore store = instantiateHStore(this.basedir, c, oldLogFile, reporter);
      stores.put(Bytes.mapKey(c.getName()), store);
      long storeSeqId = store.getMaxSequenceId();
      if (storeSeqId > maxSeqId) {
        maxSeqId = storeSeqId;
      }
    }
    
    doReconstructionLog(oldLogFile, maxSeqId, reporter);
    
    if (fs.exists(oldLogFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Deleting old log file: " + oldLogFile);
      }
      fs.delete(oldLogFile, false);
    }
    
    // Add one to the current maximum sequence id so new edits are beyond.
    this.minSequenceId = maxSeqId + 1;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Next sequence id for region " +
        Bytes.toString(regionInfo.getRegionName()) + " is " +
        this.minSequenceId);
    }

    // Get rid of any splits or merges that were lost in-progress
    Path splits = new Path(regiondir, SPLITDIR);
    if (fs.exists(splits)) {
      fs.delete(splits, true);
    }
    Path merges = new Path(regiondir, MERGEDIR);
    if (fs.exists(merges)) {
      fs.delete(merges, true);
    }

    int flushSize = regionInfo.getTableDesc().getMemcacheFlushSize();
    if (flushSize == HTableDescriptor.DEFAULT_MEMCACHE_FLUSH_SIZE) {
      flushSize = conf.getInt("hbase.hregion.memcache.flush.size",
                      HTableDescriptor.DEFAULT_MEMCACHE_FLUSH_SIZE);
    }
    this.memcacheFlushSize = flushSize;

    this.blockingMemcacheSize = this.memcacheFlushSize *
      conf.getInt("hbase.hregion.memcache.block.multiplier", 1);

    // See if region is meant to run read-only.
    if (this.regionInfo.getTableDesc().isReadOnly()) {
      this.writestate.setReadOnly(true);
    }

    // HRegion is ready to go!
    this.writestate.compacting = false;
    this.lastFlushTime = System.currentTimeMillis();
    LOG.info("region " + this + "/" + this.regionInfo.getEncodedName() +
      " available");
  }

  /**
   * @return Updates to this region need to have a sequence id that is >= to
   * the this number.
   */
  long getMinSequenceId() {
    return this.minSequenceId;
  }
  
  /** @return a HRegionInfo object for this region */
  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /** @return true if region is closed */
  public boolean isClosed() {
    return this.closed.get();
  }
  
  /**
   * Close down this HRegion.  Flush the cache, shut down each HStore, don't 
   * service any more calls.
   *
   * <p>This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   * 
   * @return Vector of all the storage files that the HRegion's component 
   * HStores make use of.  It's a list of all HStoreFile objects. Returns empty
   * vector if already closed and null if judged that it should not close.
   * 
   * @throws IOException
   */
  public List<HStoreFile> close() throws IOException {
    return close(false);
  }
  
  /**
   * Close down this HRegion.  Flush the cache unless abort parameter is true,
   * Shut down each HStore, don't service any more calls.
   *
   * This method could take some time to execute, so don't call it from a 
   * time-sensitive thread.
   * 
   * @param abort true if server is aborting (only during testing)
   * @return Vector of all the storage files that the HRegion's component 
   * HStores make use of.  It's a list of HStoreFile objects.  Can be null if
   * we are not to close at this time or we are already closed.
   * 
   * @throws IOException
   */
  List<HStoreFile> close(boolean abort) throws IOException {
    if (isClosed()) {
      LOG.warn("region " + this + " already closed");
      return null;
    }
    synchronized (splitLock) {
      synchronized (writestate) {
        // Disable compacting and flushing by background threads for this
        // region.
        writestate.writesEnabled = false;
        LOG.debug("Compactions and cache flushes disabled for region " + this);
        while (writestate.compacting || writestate.flushing) {
          LOG.debug("waiting for" +
              (writestate.compacting ? " compaction" : "") +
              (writestate.flushing ?
                  (writestate.compacting ? "," : "") + " cache flush" :
                    "") + " to complete for region " + this);
          try {
            writestate.wait();
          } catch (InterruptedException iex) {
            // continue
          }
        }
      }
      splitsAndClosesLock.writeLock().lock();
      LOG.debug("Updates and scanners disabled for region " + this);
      try {
        // Wait for active scanners to finish. The write lock we hold will
        // prevent new scanners from being created.
        synchronized (activeScannerCount) {
          while (activeScannerCount.get() != 0) {
            LOG.debug("waiting for " + activeScannerCount.get() +
                " scanners to finish");
            try {
              activeScannerCount.wait();
            } catch (InterruptedException e) {
              // continue
            }
          }
        }
        LOG.debug("No more active scanners for region " + this);

        // Write lock means no more row locks can be given out.  Wait on
        // outstanding row locks to come in before we close so we do not drop
        // outstanding updates.
        waitOnRowLocks();
        LOG.debug("No more row locks outstanding on region " + this);

        // Don't flush the cache if we are aborting
        if (!abort) {
          internalFlushcache();
        }

        List<HStoreFile> result = new ArrayList<HStoreFile>();
        for (HStore store: stores.values()) {
          result.addAll(store.close());
        }
        this.closed.set(true);
        
        LOG.info("closed " + this);
        return result;
      } finally {
        splitsAndClosesLock.writeLock().unlock();
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // HRegion accessors
  //////////////////////////////////////////////////////////////////////////////

  /** @return start key for region */
  public byte [] getStartKey() {
    return this.regionInfo.getStartKey();
  }

  /** @return end key for region */
  public byte [] getEndKey() {
    return this.regionInfo.getEndKey();
  }

  /** @return region id */
  public long getRegionId() {
    return this.regionInfo.getRegionId();
  }

  /** @return region name */
  public byte [] getRegionName() {
    return this.regionInfo.getRegionName();
  }

  /** @return HTableDescriptor for this region */
  public HTableDescriptor getTableDesc() {
    return this.regionInfo.getTableDesc();
  }

  /** @return HLog in use for this region */
  public HLog getLog() {
    return this.log;
  }

  /** @return Configuration object */
  public HBaseConfiguration getConf() {
    return this.conf;
  }

  /** @return region directory Path */
  public Path getRegionDir() {
    return this.regiondir;
  }

  /** @return FileSystem being used by this region */
  public FileSystem getFilesystem() {
    return this.fs;
  }

  /** @return the last time the region was flushed */
  public long getLastFlushTime() {
    return this.lastFlushTime;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // HRegion maintenance.  
  //
  // These methods are meant to be called periodically by the HRegionServer for 
  // upkeep.
  //////////////////////////////////////////////////////////////////////////////

  /** @return returns size of largest HStore. */
  public long getLargestHStoreSize() {
    long size = 0;
    for (HStore h: stores.values()) {
      long storeSize = h.getSize();
      if (storeSize > size) {
        size = storeSize;
      }
    }
    return size;
  }
  
  /*
   * Split the HRegion to create two brand-new ones.  This also closes
   * current HRegion.  Split should be fast since we don't rewrite store files
   * but instead create new 'reference' store files that read off the top and
   * bottom ranges of parent store files.
   * @param midKey key on which to split region
   * @return two brand-new (and open) HRegions or null if a split is not needed
   * @throws IOException
   */
  HRegion[] splitRegion(final byte [] midKey) throws IOException {
    synchronized (splitLock) {
      if (closed.get()) {
        return null;
      }
      // Add start/end key checking: hbase-428.
      byte [] startKey = this.regionInfo.getStartKey();
      byte [] endKey = this.regionInfo.getEndKey();
      if (Bytes.equals(startKey, midKey)) {
        LOG.debug("Startkey (" + startKey + ") and midkey + (" + 
          midKey + ") are same, not splitting");
        return null;
      }
      if (Bytes.equals(midKey, endKey)) {
        LOG.debug("Endkey and midkey are same, not splitting");
        return null;
      }
      LOG.info("Starting split of region " + this);
      Path splits = new Path(this.regiondir, SPLITDIR);
      if(!this.fs.exists(splits)) {
        this.fs.mkdirs(splits);
      }
      // Calculate regionid to use.  Can't be less than that of parent else
      // it'll insert into wrong location over in .META. table: HBASE-710.
      long rid = System.currentTimeMillis();
      if (rid < this.regionInfo.getRegionId()) {
        LOG.warn("Clock skew; parent regions id is " +
          this.regionInfo.getRegionId() + " but current time here is " + rid);
        rid = this.regionInfo.getRegionId() + 1;
      }
      HRegionInfo regionAInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        startKey, midKey, false, rid);
      Path dirA =
        new Path(splits, Integer.toString(regionAInfo.getEncodedName()));
      if(fs.exists(dirA)) {
        throw new IOException("Cannot split; target file collision at " + dirA);
      }
      HRegionInfo regionBInfo = new HRegionInfo(this.regionInfo.getTableDesc(),
        midKey, endKey, false, rid);
      Path dirB =
        new Path(splits, Integer.toString(regionBInfo.getEncodedName()));
      if(this.fs.exists(dirB)) {
        throw new IOException("Cannot split; target file collision at " + dirB);
      }

      // Now close the HRegion.  Close returns all store files or null if not
      // supposed to close (? What to do in this case? Implement abort of close?)
      // Close also does wait on outstanding rows and calls a flush just-in-case.
      List<HStoreFile> hstoreFilesToSplit = close(false);
      if (hstoreFilesToSplit == null) {
        LOG.warn("Close came back null (Implement abort of close?)");
        throw new RuntimeException("close returned empty vector of HStoreFiles");
      }

      // Split each store file.
      for(HStoreFile h: hstoreFilesToSplit) {
        // A reference to the bottom half of the hsf store file.
        HStoreFile.Reference aReference = new HStoreFile.Reference(
            this.regionInfo.getEncodedName(), h.getFileId(),
            new HStoreKey(midKey), HStoreFile.Range.bottom);
        HStoreFile a = new HStoreFile(this.conf, fs, splits,
            regionAInfo.getEncodedName(), h.getColFamily(), -1, aReference);
        // Reference to top half of the hsf store file.
        HStoreFile.Reference bReference = new HStoreFile.Reference(
            this.regionInfo.getEncodedName(), h.getFileId(),
            new HStoreKey(midKey), HStoreFile.Range.top);
        HStoreFile b = new HStoreFile(this.conf, fs, splits,
            regionBInfo.getEncodedName(), h.getColFamily(), -1, bReference);
        h.splitStoreFile(a, b, this.fs);
      }

      // Done!
      // Opening the region copies the splits files from the splits directory
      // under each region.
      HRegion regionA =
        new HRegion(basedir, log, fs, conf, regionAInfo, dirA, null);
      regionA.close();
      HRegion regionB =
        new HRegion(basedir, log, fs, conf, regionBInfo, dirB, null);
      regionB.close();

      // Cleanup
      boolean deleted = fs.delete(splits, true); // Get rid of splits directory
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cleaned up " + FSUtils.getPath(splits) + " " + deleted);
      }
      HRegion regions[] = new HRegion [] {regionA, regionB};
      
      this.historian.addRegionSplit(this.regionInfo,
        regionA.getRegionInfo(), regionB.getRegionInfo());
      
      return regions;
    }
  }
  
  /*
   * @param dir
   * @return compaction directory for the passed in <code>dir</code>
   */
  static Path getCompactionDir(final Path dir) {
   return new Path(dir, "compaction.dir");
  }

  /*
   * Do preparation for pending compaction.
   * Clean out any vestiges of previous failed compactions.
   * @throws IOException
   */
  private void doRegionCompactionPrep() throws IOException {
    doRegionCompactionCleanup();
  }
  
  /*
   * Removes the compaction directory for this Store.
   * @throws IOException
   */
  private void doRegionCompactionCleanup() throws IOException {
    if (this.fs.exists(this.regionCompactionDir)) {
      this.fs.delete(this.regionCompactionDir, true);
    }
  }

  /**
   * Called by compaction thread and after region is opened to compact the
   * HStores if necessary.
   *
   * <p>This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   * 
   * @return mid key if split is needed
   * @throws IOException
   */
  public byte [] compactStores() throws IOException {
    return compactStores(false);
  }

  /**
   * Called by compaction thread and after region is opened to compact the
   * HStores if necessary.
   *
   * <p>This operation could block for a long time, so don't call it from a 
   * time-sensitive thread.
   *
   * Note that no locking is necessary at this level because compaction only
   * conflicts with a region split, and that cannot happen because the region
   * server does them sequentially and not in parallel.
   * 
   * @param force True to force a compaction regardless of thresholds (Needed
   * by merge).
   * @return mid key if split is needed
   * @throws IOException
   */
  private byte [] compactStores(final boolean force) throws IOException {
    byte [] midKey = null;
    if (this.closed.get()) {
      return midKey;
    }
    try {
      synchronized (writestate) {
        if (!writestate.compacting && writestate.writesEnabled) {
          writestate.compacting = true;
        } else {
          LOG.info("NOT compacting region " + this +
              ": compacting=" + writestate.compacting + ", writesEnabled=" +
              writestate.writesEnabled);
            return midKey;
        }
      }
      LOG.info("starting compaction on region " + this);
      long startTime = System.currentTimeMillis();
      doRegionCompactionPrep();
      long maxSize = -1;
      for (HStore store: stores.values()) {
        final HStore.StoreSize size = store.compact(force);
        if (size != null && size.getSize() > maxSize) {
          maxSize = size.getSize();
          midKey = size.getKey();
        }
      }
      doRegionCompactionCleanup();
      String timeTaken = StringUtils.formatTimeDiff(System.currentTimeMillis(), 
          startTime);
      LOG.info("compaction completed on region " + this + " in " + timeTaken);
      
      this.historian.addRegionCompaction(regionInfo, timeTaken);
    } finally {
      synchronized (writestate) {
        writestate.compacting = false;
        writestate.notifyAll();
      }
    }
    return midKey;
  }

  /**
   * Flush the cache.
   * 
   * When this method is called the cache will be flushed unless:
   * <ol>
   *   <li>the cache is empty</li>
   *   <li>the region is closed.</li>
   *   <li>a flush is already in progress</li>
   *   <li>writes are disabled</li>
   * </ol>
   *
   * <p>This method may block for some time, so it should not be called from a 
   * time-sensitive thread.
   * 
   * @return true if cache was flushed
   * 
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  public boolean flushcache() throws IOException {
    if (this.closed.get()) {
      return false;
    }
    synchronized (writestate) {
      if (!writestate.flushing && writestate.writesEnabled) {
        this.writestate.flushing = true;
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug("NOT flushing memcache for region " + this +
            ", flushing=" +
              writestate.flushing + ", writesEnabled=" +
              writestate.writesEnabled);
        }
        return false;  
      }
    }
    try {
      // Prevent splits and closes
      splitsAndClosesLock.readLock().lock();
      try {
        return internalFlushcache();
      } finally {
        splitsAndClosesLock.readLock().unlock();
      }
    } finally {
      synchronized (writestate) {
        writestate.flushing = false;
        this.writestate.flushRequested = false;
        writestate.notifyAll();
      }
    }
  }

  /**
   * Flushing the cache is a little tricky. We have a lot of updates in the
   * HMemcache, all of which have also been written to the log. We need to
   * write those updates in the HMemcache out to disk, while being able to
   * process reads/writes as much as possible during the flush operation. Also,
   * the log has to state clearly the point in time at which the HMemcache was
   * flushed. (That way, during recovery, we know when we can rely on the
   * on-disk flushed structures and when we have to recover the HMemcache from
   * the log.)
   * 
   * <p>So, we have a three-step process:
   * 
   * <ul><li>A. Flush the memcache to the on-disk stores, noting the current
   * sequence ID for the log.<li>
   * 
   * <li>B. Write a FLUSHCACHE-COMPLETE message to the log, using the sequence
   * ID that was current at the time of memcache-flush.</li>
   * 
   * <li>C. Get rid of the memcache structures that are now redundant, as
   * they've been flushed to the on-disk HStores.</li>
   * </ul>
   * <p>This method is protected, but can be accessed via several public
   * routes.
   * 
   * <p> This method may block for some time.
   * 
   * @return true if the region needs compacting
   * 
   * @throws IOException
   * @throws DroppedSnapshotException Thrown when replay of hlog is required
   * because a Snapshot was not properly persisted.
   */
  private boolean internalFlushcache() throws IOException {
    final long startTime = System.currentTimeMillis();
    // Clear flush flag.
    // Record latest flush time
    this.lastFlushTime = startTime;
    // If nothing to flush, return and avoid logging start/stop flush.
    if (this.memcacheSize.get() <= 0) {
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started memcache flush for region " + this +
        ". Current region memcache size " +
          StringUtils.humanReadableInt(this.memcacheSize.get()));
    }

    // Stop updates while we snapshot the memcache of all stores. We only have
    // to do this for a moment.  Its quick.  The subsequent sequence id that
    // goes into the HLog after we've flushed all these snapshots also goes
    // into the info file that sits beside the flushed files.
    // We also set the memcache size to zero here before we allow updates
    // again so its value will represent the size of the updates received
    // during the flush
    long sequenceId = -1L;
    this.updatesLock.writeLock().lock();
    try {
      for (HStore s: stores.values()) {
        s.snapshot();
      }
      sequenceId = log.startCacheFlush();
      this.memcacheSize.set(0);
    } finally {
      this.updatesLock.writeLock().unlock();
    }

    // Any failure from here on out will be catastrophic requiring server
    // restart so hlog content can be replayed and put back into the memcache.
    // Otherwise, the snapshot content while backed up in the hlog, it will not
    // be part of the current running servers state.
    boolean compactionRequested = false;
    try {
      // A.  Flush memcache to all the HStores.
      // Keep running vector of all store files that includes both old and the
      // just-made new flush store file.
      for (HStore hstore: stores.values()) {
        boolean needsCompaction = hstore.flushCache(sequenceId);
        if (needsCompaction) {
          compactionRequested = true;
        }
      }
    } catch (Throwable t) {
      // An exception here means that the snapshot was not persisted.
      // The hlog needs to be replayed so its content is restored to memcache.
      // Currently, only a server restart will do this.
      // We used to only catch IOEs but its possible that we'd get other
      // exceptions -- e.g. HBASE-659 was about an NPE -- so now we catch
      // all and sundry.
      this.log.abortCacheFlush();
      DroppedSnapshotException dse = new DroppedSnapshotException("region: " +
          Bytes.toString(getRegionName()));
      dse.initCause(t);
      throw dse;
    }

    // If we get to here, the HStores have been written. If we get an
    // error in completeCacheFlush it will release the lock it is holding

    // B.  Write a FLUSHCACHE-COMPLETE message to the log.
    //     This tells future readers that the HStores were emitted correctly,
    //     and that all updates to the log for this regionName that have lower 
    //     log-sequence-ids can be safely ignored.
    this.log.completeCacheFlush(getRegionName(),
        regionInfo.getTableDesc().getName(), sequenceId);

    // C. Finally notify anyone waiting on memcache to clear:
    // e.g. checkResources().
    synchronized (this) {
      notifyAll();
    }
    
    if (LOG.isDebugEnabled()) {
      String timeTaken = StringUtils.formatTimeDiff(System.currentTimeMillis(), 
          startTime);
      LOG.debug("Finished memcache flush for region " + this +
        " in " +
          (System.currentTimeMillis() - startTime) + "ms, sequence id=" +
          sequenceId + ", compaction requested=" + compactionRequested);
      if (!regionInfo.isMetaRegion()) {
        this.historian.addRegionFlush(regionInfo, timeTaken);
      }
    }
    return compactionRequested;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // get() methods for client use.
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Fetch a single data item.
   * @param row
   * @param column
   * @return column value
   * @throws IOException
   */
  public Cell get(byte [] row, byte [] column) throws IOException {
    Cell[] results = get(row, column, Long.MAX_VALUE, 1);
    return (results == null || results.length == 0)? null: results[0];
  }
  /**
   * Fetch multiple versions of a single data item
   * 
   * @param row
   * @param column
   * @param numVersions
   * @return array of values one element per version
   * @throws IOException
   */
  public Cell[] get(byte [] row, byte [] column, int numVersions)
  throws IOException {
    return get(row, column, Long.MAX_VALUE, numVersions);
  }

  /**
   * Fetch multiple versions of a single data item, with timestamp.
   *
   * @param row
   * @param column
   * @param timestamp
   * @param numVersions
   * @return array of values one element per version that matches the timestamp
   * @throws IOException
   */
  public Cell[] get(byte [] row, byte [] column, long timestamp,
    int numVersions) 
  throws IOException {
    if (this.closed.get()) {
      throw new IOException("Region " + this + " closed");
    }

    // Make sure this is a valid row and valid column
    checkRow(row);
    checkColumn(column);
    // Don't need a row lock for a simple get
    HStoreKey key = new HStoreKey(row, column, timestamp);
    return getStore(column).get(key, numVersions);
  }

  /**
   * Fetch all the columns for the indicated row at a specified timestamp.
   * Returns a TreeMap that maps column names to values.
   *
   * We should eventually use Bloom filters here, to reduce running time.  If 
   * the database has many column families and is very sparse, then we could be 
   * checking many files needlessly.  A small Bloom for each row would help us 
   * determine which column groups are useful for that row.  That would let us 
   * avoid a bunch of disk activity.
   *
   * @param row
   * @param columns Array of columns you'd like to retrieve. When null, get all.
   * @param ts
   * @return Map<columnName, Cell> values
   * @throws IOException
   */
  public Map<byte [], Cell> getFull(final byte [] row,
      final Set<byte []> columns, final long ts, final Integer lockid) 
  throws IOException {
    // Check columns passed
    if (columns != null) {
      for (byte [] column: columns) {
        checkColumn(column);
      }
    }
    HStoreKey key = new HStoreKey(row, ts);
    Integer lid = getLock(lockid,row);
    HashSet<HStore> storeSet = new HashSet<HStore>();
    try {
      TreeMap<byte [], Cell> result =
        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
      // Get the concerned columns or all of them
      if (columns != null) {
        for (byte[] bs : columns) {
          HStore store = stores.get(Bytes.mapKey(HStoreKey.getFamily(bs)));
          if (store != null) {
            storeSet.add(store);
          }
        }
      } else {
        storeSet.addAll(stores.values());
      }
      // For each column name that is just a column family, open the store
      // related to it and fetch everything for that row. HBASE-631
      // Also remove each store from storeSet so that these stores
      // won't be opened for no reason. HBASE-783
      if (columns != null) {
        for (byte[] bs : columns) {
          if (HStoreKey.getFamilyDelimiterIndex(bs) == (bs.length - 1)) {
            HStore store = stores.get(Bytes.mapKey(HStoreKey.getFamily(bs)));
            store.getFull(key, null, result);
            storeSet.remove(store);
          }
        }
      }
      
      for (HStore targetStore: storeSet) {
        targetStore.getFull(key, columns, result);
      }
      
      return result;
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Return all the data for the row that matches <i>row</i> exactly, 
   * or the one that immediately preceeds it, at or immediately before 
   * <i>ts</i>.
   * 
   * @param row row key
   * @return map of values
   * @throws IOException
   */
  public RowResult getClosestRowBefore(final byte [] row)
  throws IOException{
    // look across all the HStores for this region and determine what the
    // closest key is across all column families, since the data may be sparse
    HStoreKey key = null;
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      // examine each column family for the preceeding or matching key
      for (HStore store : stores.values()) {
        // get the closest key
        byte [] closestKey = store.getRowKeyAtOrBefore(row);
        // if it happens to be an exact match, we can stop looping
        if (Bytes.equals(row, closestKey)) {
          key = new HStoreKey(closestKey);
          break;
        }
        // otherwise, we need to check if it's the max and move to the next
        if (closestKey != null 
          && (key == null || Bytes.compareTo(closestKey, key.getRow()) > 0) ) {
          key = new HStoreKey(closestKey);
        }
      }
      if (key == null) {
        return null;
      }
      
      // now that we've found our key, get the values
      HbaseMapWritable<byte [], Cell> cells =
        new HbaseMapWritable<byte [], Cell>();
      for (HStore s: stores.values()) {
        s.getFull(key, null, cells);
      }
      return new RowResult(key.getRow(), cells);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all.
   * @return Ordered list of <code>versions</code> keys going from newest back.
   * @throws IOException
   */
  private Set<HStoreKey> getKeys(final HStoreKey origin, final int versions)
  throws IOException {
    Set<HStoreKey> keys = new TreeSet<HStoreKey>();
    Collection<HStore> storesToCheck = null;
    if (origin.getColumn() == null || origin.getColumn().length == 0) {
      // All families
      storesToCheck = this.stores.values();
    } else {
      storesToCheck = new ArrayList<HStore>(1);
      storesToCheck.add(getStore(origin.getColumn()));
    }
    for (HStore targetStore: storesToCheck) {
      if (targetStore != null) {
        // Pass versions without modification since in the store getKeys, it
        // includes the size of the passed <code>keys</code> array when counting.
        List<HStoreKey> r = targetStore.getKeys(origin, versions);
        if (r != null) {
          keys.addAll(r);
        }
      }
    }
    return keys;
  }
  
  /**
   * Return an iterator that scans over the HRegion, returning the indicated 
   * columns for only the rows that match the data filter.  This Iterator must
   * be closed by the caller.
   *
   * @param cols columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param firstRow row which is the starting point of the scan
   * @param timestamp only return rows whose timestamp is <= this value
   * @param filter row filter
   * @return InternalScanner
   * @throws IOException
   */
  public InternalScanner getScanner(byte[][] cols, byte [] firstRow,
    long timestamp, RowFilterInterface filter) 
  throws IOException {
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new IOException("Region " + this + " closed");
      }
      HashSet<HStore> storeSet = new HashSet<HStore>();
      for (int i = 0; i < cols.length; i++) {
        HStore s = stores.get(Bytes.mapKey(HStoreKey.getFamily(cols[i])));
        if (s != null) {
          storeSet.add(s);
        }
      }
      return new HScanner(cols, firstRow, timestamp,
        storeSet.toArray(new HStore [storeSet.size()]), filter);
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // set() methods for client use.
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * @param b
   * @throws IOException
   */
  public void batchUpdate(BatchUpdate b, Integer lockid)
  throws IOException {
    checkReadOnly();

    // Do a rough check that we have resources to accept a write.  The check is
    // 'rough' in that between the resource check and the call to obtain a 
    // read lock, resources may run out.  For now, the thought is that this
    // will be extremely rare; we'll deal with it when it happens.
    checkResources();

    // We obtain a per-row lock, so other clients will block while one client
    // performs an update. The read lock is released by the client calling
    // #commit or #abort or if the HRegionServer lease on the lock expires.
    // See HRegionServer#RegionListener for how the expire on HRegionServer
    // invokes a HRegion#abort.
    byte [] row = b.getRow();
    // If we did not pass an existing row lock, obtain a new one
    Integer lid = getLock(lockid,row);
    long commitTime = (b.getTimestamp() == LATEST_TIMESTAMP) ?
      System.currentTimeMillis() : b.getTimestamp();
    try {
      List<byte []> deletes = null;
      for (BatchOperation op: b) {
        HStoreKey key = new HStoreKey(row, op.getColumn(), commitTime);
        byte[] val = null;
        if (op.isPut()) {
          val = op.getValue();
          if (HLogEdit.isDeleted(val)) {
            throw new IOException("Cannot insert value: " + val);
          }
        } else {
          if (b.getTimestamp() == LATEST_TIMESTAMP) {
            // Save off these deletes
            if (deletes == null) {
              deletes = new ArrayList<byte []>();
            }
            deletes.add(op.getColumn());
          } else {
            val = HLogEdit.deleteBytes.get();
          }
        }
        if (val != null) {
          localput(lid, key, val);
        }
      }
      TreeMap<HStoreKey, byte[]> edits =
        this.targetColumns.remove(lid);

      if (edits != null && edits.size() > 0) {
        update(edits);
      }
      
      if (deletes != null && deletes.size() > 0) {
        // We have some LATEST_TIMESTAMP deletes to run.
        for (byte [] column: deletes) {
          deleteMultiple(row, column, LATEST_TIMESTAMP, 1);
        }
      }
    } catch (IOException e) {
      this.targetColumns.remove(Long.valueOf(lid));
      throw e;
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /*
   * Check if resources to support an update.
   * 
   * For now, just checks memcache saturation.
   * 
   * Here we synchronize on HRegion, a broad scoped lock.  Its appropriate
   * given we're figuring in here whether this region is able to take on
   * writes.  This is only method with a synchronize (at time of writing),
   * this and the synchronize on 'this' inside in internalFlushCache to send
   * the notify.
   */
  private void checkResources() {
    if (this.memcacheSize.get() > this.blockingMemcacheSize) {
      requestFlush();
      doBlocking();
    }
  }
  
  private synchronized void doBlocking() {
    boolean blocked = false;
    while (this.memcacheSize.get() > this.blockingMemcacheSize) {
      if (!blocked) {
        LOG.info("Blocking updates for '" + Thread.currentThread().getName() +
            "' on region " + Bytes.toString(getRegionName()) + ": Memcache size " +
            StringUtils.humanReadableInt(this.memcacheSize.get()) +
            " is >= than blocking " +
            StringUtils.humanReadableInt(this.blockingMemcacheSize) + " size");
      }
      blocked = true;
      try {
        wait(threadWakeFrequency);
      } catch (InterruptedException e) {
        // continue;
      }
    }
    if (blocked) {
      LOG.info("Unblocking updates for region " + this + " '" + 
        Thread.currentThread().getName() + "'");
    }
  }
  
  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param column
   * @param ts Delete all entries that have this timestamp or older
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final byte [] column, final long ts,
      final Integer lockid)
  throws IOException {
    checkColumn(column);
    checkReadOnly();
    Integer lid = getLock(lockid,row);
    try {
      deleteMultiple(row, column, ts, ALL_VERSIONS);
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Delete all cells of the same age as the passed timestamp or older.
   * @param row
   * @param ts Delete all entries that have this timestamp or older
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteAll(final byte [] row, final long ts,
      final Integer lockid)
  throws IOException {
    checkReadOnly();
    Integer lid = getLock(lockid,row);
    try {
      for (HStore store : stores.values()){
        List<HStoreKey> keys = store.getKeys(new HStoreKey(row, ts),
          ALL_VERSIONS);
        TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
        for (HStoreKey key: keys) {
          edits.put(key, HLogEdit.deleteBytes.get());
        }
        update(edits);
      }
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }

  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @param lockid Row lock
   * @throws IOException
   */
  public void deleteFamily(byte [] row, byte [] family, long timestamp,
      final Integer lockid)
  throws IOException{
    checkReadOnly();
    Integer lid = getLock(lockid,row);
    try {
      // find the HStore for the column family
      HStore store = getStore(family);
      // find all the keys that match our criteria
      List<HStoreKey> keys = store.getKeys(new HStoreKey(row, timestamp), ALL_VERSIONS);
      // delete all the cells
      TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
      for (HStoreKey key: keys) {
        edits.put(key, HLogEdit.deleteBytes.get());
      }
      update(edits);
    } finally {
      if(lockid == null) releaseRowLock(lid);
    }
  }
  
  /**
   * Delete one or many cells.
   * Used to support {@link #deleteAll(byte [], byte [], long)} and deletion of
   * latest cell.
   * 
   * @param row
   * @param column
   * @param ts Timestamp to start search on.
   * @param versions How many versions to delete. Pass
   * {@link HConstants#ALL_VERSIONS} to delete all.
   * @throws IOException
   */
  private void deleteMultiple(final byte [] row, final byte [] column,
      final long ts, final int versions)
  throws IOException {
    checkReadOnly();
    HStoreKey origin = new HStoreKey(row, column, ts);
    Set<HStoreKey> keys = getKeys(origin, versions);
    if (keys.size() > 0) {
      TreeMap<HStoreKey, byte []> edits = new TreeMap<HStoreKey, byte []>();
      for (HStoreKey key: keys) {
        edits.put(key, HLogEdit.deleteBytes.get());
      }
      update(edits);
    }
  }
    
  /**
   * @throws IOException Throws exception if region is in read-only mode.
   */
  protected void checkReadOnly() throws IOException {
    if (this.writestate.isReadOnly()) {
      throw new IOException("region is read only");
    }
  }
  
  /**
   * Private implementation.
   * 
   * localput() is used for both puts and deletes. We just place the values
   * into a per-row pending area, until a commit() or abort() call is received.
   * (Or until the user's write-lock expires.)
   * 
   * @param lockid
   * @param key 
   * @param val Value to enter into cell
   * @throws IOException
   */
  private void localput(final Integer lockid, final HStoreKey key,
      final byte [] val)
  throws IOException {
    checkColumn(key.getColumn());
    checkReadOnly();
    TreeMap<HStoreKey, byte []> targets = this.targetColumns.get(lockid);
    if (targets == null) {
      targets = new TreeMap<HStoreKey, byte []>();
      this.targetColumns.put(lockid, targets);
    }
    targets.put(key, val);
  }

  /* 
   * Add updates first to the hlog and then add values to memcache.
   * Warning: Assumption is caller has lock on passed in row.
   * @param row Row to update.
   * @param timestamp Timestamp to record the updates against
   * @param updatesByColumn Cell updates by column
   * @throws IOException
   */
  private void update(final TreeMap<HStoreKey, byte []> updatesByColumn)
  throws IOException {
    if (updatesByColumn == null || updatesByColumn.size() <= 0) {
      return;
    }
    checkReadOnly();
    boolean flush = false;
    this.updatesLock.readLock().lock();
    try {
      this.log.append(regionInfo.getRegionName(),
        regionInfo.getTableDesc().getName(), updatesByColumn);
      long size = 0;
      for (Map.Entry<HStoreKey, byte[]> e: updatesByColumn.entrySet()) {
        HStoreKey key = e.getKey();
        size = this.memcacheSize.addAndGet(
            getStore(key.getColumn()).add(key, e.getValue()));
      }
      flush = isFlushSize(size);
    } finally {
      this.updatesLock.readLock().unlock();
    }
    if (flush) {
      // Request a cache flush.  Do it outside update lock.
      requestFlush();
    }
  }

  private void requestFlush() {
    if (this.flushListener == null) {
      return;
    }
    synchronized (writestate) {
      if (this.writestate.isFlushRequested()) {
        return;
      }
      writestate.flushRequested = true;
    }
    // Make request outside of synchronize block; HBASE-818.
    this.flushListener.request(this);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flush requested on " + this);
    }
  }

  /*
   * @param size
   * @return True if size is over the flush threshold
   */
  private boolean isFlushSize(final long size) {
    return size > this.memcacheFlushSize;
  }
  
  // Do any reconstruction needed from the log
  @SuppressWarnings("unused")
  protected void doReconstructionLog(Path oldLogFile, long maxSeqId,
    Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    // Nothing to do (Replaying is done in HStores)
  }

  protected HStore instantiateHStore(Path baseDir, 
    HColumnDescriptor c, Path oldLogFile, Progressable reporter)
  throws IOException {
    return new HStore(baseDir, this.regionInfo, c, this.fs, oldLogFile,
      this.conf, reporter);
  }

  /*
   * @param column
   * @return Store that goes with the family on passed <code>column</code>.
   * TODO: Make this lookup faster.
   */
  protected HStore getStore(final byte [] column) {
    return this.stores.get(HStoreKey.getFamilyMapKey(column)); 
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Support code
  //////////////////////////////////////////////////////////////////////////////

  /** Make sure this is a valid row for the HRegion */
  private void checkRow(final byte [] row) throws IOException {
    if(!rowIsInRange(regionInfo, row)) {
      throw new WrongRegionException("Requested row out of range for " +
          "HRegion " + this + ", startKey='" +
          Bytes.toString(regionInfo.getStartKey()) + "', getEndKey()='" +
          Bytes.toString(regionInfo.getEndKey()) + "', row='" +
          Bytes.toString(row) + "'");
    }
  }
  
  /*
   * Make sure this is a valid column for the current table
   * @param columnName
   * @throws NoSuchColumnFamilyException
   */
  private void checkColumn(final byte [] columnName)
  throws NoSuchColumnFamilyException, ColumnNameParseException {
    if (columnName == null) {
      return;
    }

    int index = HStoreKey.getFamilyDelimiterIndex(columnName);
    if (index <= 0) {
      throw new ColumnNameParseException(Bytes.toString(columnName) +
        " is missing column family delimiter '" +
        HStoreKey.COLUMN_FAMILY_DELIMITER + "'");
    }
    if (!regionInfo.getTableDesc().hasFamily(columnName, index)) {
      throw new NoSuchColumnFamilyException("Column family on " +
        Bytes.toString(columnName) + " does not exist in region " + this
          + " in table " + regionInfo.getTableDesc());
    }
  }

  /**
   * Obtain a lock on the given row.  Blocks until success.
   *
   * I know it's strange to have two mappings:
   * <pre>
   *   ROWS  ==> LOCKS
   * </pre>
   * as well as
   * <pre>
   *   LOCKS ==> ROWS
   * </pre>
   *
   * But it acts as a guard on the client; a miswritten client just can't
   * submit the name of a row and start writing to it; it must know the correct
   * lockid, which matches the lock list in memory.
   * 
   * <p>It would be more memory-efficient to assume a correctly-written client, 
   * which maybe we'll do in the future.
   * 
   * @param row Name of row to lock.
   * @throws IOException
   * @return The id of the held lock.
   */
  Integer obtainRowLock(final byte [] row) throws IOException {
    checkRow(row);
    splitsAndClosesLock.readLock().lock();
    try {
      if (this.closed.get()) {
        throw new NotServingRegionException("Region " + this + " closed");
      }
      Integer key = Bytes.mapKey(row);
      synchronized (locksToRows) {
        while (locksToRows.containsKey(key)) {
          try {
            locksToRows.wait();
          } catch (InterruptedException ie) {
            // Empty
          }
        }
        locksToRows.put(key, row);
        locksToRows.notifyAll();
        return key;
      }
    } finally {
      splitsAndClosesLock.readLock().unlock();
    }
  }
  
  /**
   * Used by unit tests.
   * @param lockid
   * @return Row that goes with <code>lockid</code>
   */
  byte [] getRowFromLock(final Integer lockid) {
    return locksToRows.get(lockid);
  }
  
  /** 
   * Release the row lock!
   * @param row Name of row whose lock we are to release
   */
  void releaseRowLock(final Integer lockid) {
    synchronized (locksToRows) {
      locksToRows.remove(lockid);
      locksToRows.notifyAll();
    }
  }
  
  /**
   * See if row is currently locked.
   * @param lockid
   * @return boolean
   */
  private boolean isRowLocked(final Integer lockid) {
    synchronized (locksToRows) {
      if(locksToRows.containsKey(lockid)) {
        return true;
      } else {
        return false;
      }
    }
  }
  
  /**
   * Returns existing row lock if found, otherwise
   * obtains a new row lock and returns it.
   * @param lockid
   * @return lockid
   */
  private Integer getLock(Integer lockid, byte [] row) 
  throws IOException {
    Integer lid = null;
    if(lockid == null) {
      lid = obtainRowLock(row);
    } else {
      if(!isRowLocked(lockid)) {
        throw new IOException("Invalid row lock");
      }
      lid = lockid;
    }
    return lid;
  }
  
  private void waitOnRowLocks() {
    synchronized (locksToRows) {
      while (this.locksToRows.size() > 0) {
        LOG.debug("waiting for " + this.locksToRows.size() + " row locks");
        try {
          this.locksToRows.wait();
        } catch (InterruptedException e) {
          // Catch. Let while test determine loop-end.
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    return this.hashCode() == ((HRegion)o).hashCode();
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return this.regionInfo.getRegionName().hashCode();
  }
  
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.regionInfo.getRegionNameAsString();
  }

  /** @return Path of region base directory */
  public Path getBaseDir() {
    return this.basedir;
  }

  /**
   * HScanner is an iterator through a bunch of rows in an HRegion.
   */
  private class HScanner implements InternalScanner {
    private InternalScanner[] scanners;
    private TreeMap<byte [], Cell>[] resultSets;
    private HStoreKey[] keys;
    private RowFilterInterface filter;

    /** Create an HScanner with a handle on many HStores. */
    @SuppressWarnings("unchecked")
    HScanner(byte [][] cols, byte [] firstRow, long timestamp, HStore[] stores,
      RowFilterInterface filter)
    throws IOException {
      this.filter = filter;
      this.scanners = new InternalScanner[stores.length];
      try {
        for (int i = 0; i < stores.length; i++) {
          
          // Only pass relevant columns to each store
          
          List<byte[]> columns = new ArrayList<byte[]>();
          for (int j = 0; j < cols.length; j++) {
            if (Bytes.equals(HStoreKey.getFamily(cols[j]),
                stores[i].getFamily().getName())) {
              columns.add(cols[j]);
            }
          }

          RowFilterInterface f = filter;
          if (f != null) {
            // Need to replicate filters.
            // At least WhileMatchRowFilter will mess up the scan if only
            // one shared across many rows. See HADOOP-2467.
            f = (RowFilterInterface)WritableUtils.clone(filter, conf);
          }
          scanners[i] = stores[i].getScanner(timestamp,
              columns.toArray(new byte[columns.size()][]), firstRow, f);
        }
      } catch (IOException e) {
        for (int i = 0; i < this.scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
        throw e;
      }

      // Advance to the first key in each store.
      // All results will match the required column-set and scanTime.
      this.resultSets = new TreeMap[scanners.length];
      this.keys = new HStoreKey[scanners.length];
      for (int i = 0; i < scanners.length; i++) {
        keys[i] = new HStoreKey();
        resultSets[i] = new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
        if(scanners[i] != null && !scanners[i].next(keys[i], resultSets[i])) {
          closeScanner(i);
        }
      }

      // As we have now successfully completed initialization, increment the
      // activeScanner count.
      activeScannerCount.incrementAndGet();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("null")
    public boolean next(HStoreKey key, SortedMap<byte [], Cell> results)
    throws IOException {
      boolean moreToFollow = false;
      boolean filtered = false;

      do {
        // Find the lowest-possible key.
        byte [] chosenRow = null;
        long chosenTimestamp = -1;
        for (int i = 0; i < this.keys.length; i++) {
          if (scanners[i] != null &&
              (chosenRow == null ||
                  (Bytes.compareTo(keys[i].getRow(), chosenRow) < 0) ||
                  ((Bytes.compareTo(keys[i].getRow(), chosenRow) == 0) &&
                      (keys[i].getTimestamp() > chosenTimestamp)))) {
            chosenRow = keys[i].getRow();
            chosenTimestamp = keys[i].getTimestamp();
          }
        }

        // Store the key and results for each sub-scanner. Merge them as
        // appropriate.
        if (chosenTimestamp >= 0) {
          // Here we are setting the passed in key with current row+timestamp
          key.setRow(chosenRow);
          key.setVersion(chosenTimestamp);
          key.setColumn(HConstants.EMPTY_BYTE_ARRAY);

          for (int i = 0; i < scanners.length; i++) {
            if (scanners[i] != null &&
              Bytes.compareTo(keys[i].getRow(), chosenRow) == 0) {
              // NOTE: We used to do results.putAll(resultSets[i]);
              // but this had the effect of overwriting newer
              // values with older ones. So now we only insert
              // a result if the map does not contain the key.
              for (Map.Entry<byte [], Cell> e : resultSets[i].entrySet()) {
                if (!results.containsKey(e.getKey())) {
                  results.put(e.getKey(), e.getValue());
                }
              }
              resultSets[i].clear();
              if (!scanners[i].next(keys[i], resultSets[i])) {
                closeScanner(i);
              }
            }
          }
        }

        for (int i = 0; i < scanners.length; i++) {
          // If the current scanner is non-null AND has a lower-or-equal
          // row label, then its timestamp is bad. We need to advance it.
          while ((scanners[i] != null) &&
              (Bytes.compareTo(keys[i].getRow(), chosenRow) <= 0)) {
            resultSets[i].clear();
            if (!scanners[i].next(keys[i], resultSets[i])) {
              closeScanner(i);
            }
          }
        }

        moreToFollow = chosenTimestamp >= 0;
        if (results == null || results.size() <= 0) {
          // If we got no results, then there is no more to follow.
          moreToFollow = false;
        }
        
        filtered = filter == null ? false : filter.filterRow(results);
        
        if (filter != null && filter.filterAllRemaining()) {
          moreToFollow = false;
        }
        
        if (moreToFollow) {
          if (filter != null) {
            filter.rowProcessed(filtered, key.getRow());
          }
          if (filtered) {
            results.clear();
          }
        }
      } while(filtered && moreToFollow);

      // Make sure scanners closed if no more results
      if (!moreToFollow) {
        for (int i = 0; i < scanners.length; i++) {
          if (null != scanners[i]) {
            closeScanner(i);
          }
        }
      }
      
      return moreToFollow;
    }

    /** Shut down a single scanner */
    void closeScanner(int i) {
      try {
        try {
          scanners[i].close();
        } catch (IOException e) {
          LOG.warn("Failed closing scanner " + i, e);
        }
      } finally {
        scanners[i] = null;
        // These data members can be null if exception in constructor
        if (resultSets != null) {
          resultSets[i] = null;
        }
        if (keys != null) {
          keys[i] = null;
        }
      }
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
      try {
        for(int i = 0; i < scanners.length; i++) {
          if(scanners[i] != null) {
            closeScanner(i);
          }
        }
      } finally {
        synchronized (activeScannerCount) {
          int count = activeScannerCount.decrementAndGet();
          if (count < 0) {
            LOG.error("active scanner count less than zero: " + count +
                " resetting to zero");
            activeScannerCount.set(0);
            count = 0;
          }
          if (count == 0) {
            activeScannerCount.notifyAll();
          }
        }
      }
    }

    /** {@inheritDoc} */
    public boolean isWildcardScanner() {
      throw new UnsupportedOperationException("Unimplemented on HScanner");
    }

    /** {@inheritDoc} */
    public boolean isMultipleMatchScanner() {
      throw new UnsupportedOperationException("Unimplemented on HScanner");
    }  
  }
  
  // Utility methods

  /**
   * Convenience method creating new HRegions. Used by createTable and by the
   * bootstrap code in the HMaster constructor.
   * Note, this method creates an {@link HLog} for the created region. It
   * needs to be closed explicitly.  Use {@link HRegion#getLog()} to get
   * access.
   * @param info Info for region to create.
   * @param rootDir Root directory for HBase instance
   * @param conf
   * @return new HRegion
   * 
   * @throws IOException
   */
  public static HRegion createHRegion(final HRegionInfo info, final Path rootDir,
    final HBaseConfiguration conf)
  throws IOException {
    Path tableDir =
      HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName());
    Path regionDir = HRegion.getRegionDir(tableDir, info.getEncodedName());
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(regionDir);
    // Note in historian the creation of new region.
    if (!info.isMetaRegion()) {
      RegionHistorian.getInstance().addRegionCreation(info);
    }
    return new HRegion(tableDir,
      new HLog(fs, new Path(regionDir, HREGION_LOGDIR_NAME), conf, null),
      fs, conf, info, null, null);
  }
  
  /**
   * Convenience method to open a HRegion outside of an HRegionServer context.
   * @param info Info for region to be opened.
   * @param rootDir Root directory for HBase instance
   * @param log HLog for region to use. This method will call
   * HLog#setSequenceNumber(long) passing the result of the call to
   * HRegion#getMinSequenceId() to ensure the log id is properly kept
   * up.  HRegionStore does this every time it opens a new region.
   * @param conf
   * @return new HRegion
   * 
   * @throws IOException
   */
  public static HRegion openHRegion(final HRegionInfo info, final Path rootDir,
    final HLog log, final HBaseConfiguration conf)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening region: " + info);
    }
    if (info == null) {
      throw new NullPointerException("Passed region info is null");
    }
    HRegion r = new HRegion(
        HTableDescriptor.getTableDir(rootDir, info.getTableDesc().getName()),
        log, FileSystem.get(conf), conf, info, null, null);
    if (log != null) {
      log.setSequenceNumber(r.getMinSequenceId());
    }
    return r;
  }
  
  /**
   * Inserts a new region's meta information into the passed
   * <code>meta</code> region. Used by the HMaster bootstrap code adding
   * new table to ROOT table.
   * 
   * @param meta META HRegion to be updated
   * @param r HRegion to add to <code>meta</code>
   *
   * @throws IOException
   */
  public static void addRegionToMETA(HRegion meta, HRegion r) 
  throws IOException {
    meta.checkResources();
    // The row key is the region name
    byte [] row = r.getRegionName();
    Integer lid = meta.obtainRowLock(row);
    try {
      HStoreKey key = new HStoreKey(row, COL_REGIONINFO, System.currentTimeMillis());
      TreeMap<HStoreKey, byte[]> edits = new TreeMap<HStoreKey, byte[]>();
      edits.put(key, Writables.getBytes(r.getRegionInfo()));
      meta.update(edits);
    } finally {
      meta.releaseRowLock(lid);
    }
  }

  /**
   * Delete a region's meta information from the passed
   * <code>meta</code> region.
   * 
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param regionName HRegion to remove from <code>meta</code>
   *
   * @throws IOException
   */
  public static void removeRegionFromMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final byte [] regionName)
  throws IOException {
    srvr.deleteAll(metaRegionName, regionName, HConstants.LATEST_TIMESTAMP,
        (long)-1L);
  }

  /**
   * Utility method used by HMaster marking regions offlined.
   * @param srvr META server to be updated
   * @param metaRegionName Meta region name
   * @param info HRegion to update in <code>meta</code>
   *
   * @throws IOException
   */
  public static void offlineRegionInMETA(final HRegionInterface srvr,
    final byte [] metaRegionName, final HRegionInfo info)
  throws IOException {
    BatchUpdate b = new BatchUpdate(info.getRegionName());
    info.setOffline(true);
    b.put(COL_REGIONINFO, Writables.getBytes(info));
    b.delete(COL_SERVER);
    b.delete(COL_STARTCODE);
    // If carrying splits, they'll be in place when we show up on new
    // server.
    srvr.batchUpdate(metaRegionName, b, (long)-1L);
  }

  /**
   * Deletes all the files for a HRegion
   * 
   * @param fs the file system object
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for region to be deleted
   * @throws IOException
   */
  public static void deleteRegion(FileSystem fs, Path rootdir, HRegionInfo info)
  throws IOException {
    deleteRegion(fs, HRegion.getRegionDir(rootdir, info));
  }

  private static void deleteRegion(FileSystem fs, Path regiondir)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regiondir.toString());
    }
    fs.delete(regiondir, true);
  }

  /**
   * Computes the Path of the HRegion
   * 
   * @param tabledir qualified path for table
   * @param name ENCODED region name
   * @return Path of HRegion directory
   */
  public static Path getRegionDir(final Path tabledir, final int name) {
    return new Path(tabledir, Integer.toString(name));
  }
  
  /**
   * Computes the Path of the HRegion
   * 
   * @param rootdir qualified path of HBase root directory
   * @param info HRegionInfo for the region
   * @return qualified path of region directory
   */
  public static Path getRegionDir(final Path rootdir, final HRegionInfo info) {
    return new Path(
      HTableDescriptor.getTableDir(rootdir, info.getTableDesc().getName()),
      Integer.toString(info.getEncodedName()));
  }

  /**
   * Determines if the specified row is within the row range specified by the
   * specified HRegionInfo
   *  
   * @param info HRegionInfo that specifies the row range
   * @param row row to be checked
   * @return true if the row is within the range specified by the HRegionInfo
   */
  public static boolean rowIsInRange(HRegionInfo info, final byte [] row) {
    return ((info.getStartKey().length == 0) ||
        (Bytes.compareTo(info.getStartKey(), row) <= 0)) &&
        ((info.getEndKey().length == 0) ||
            (Bytes.compareTo(info.getEndKey(), row) > 0));
  }

  /**
   * Make the directories for a specific column family
   * 
   * @param fs the file system
   * @param basedir base directory where region will live (usually the table dir)
   * @param encodedRegionName encoded region name
   * @param colFamily the column family
   * @param tabledesc table descriptor of table
   * @throws IOException
   */
  public static void makeColumnFamilyDirs(FileSystem fs, Path basedir,
    int encodedRegionName, byte [] colFamily, HTableDescriptor tabledesc)
  throws IOException {
    fs.mkdirs(HStoreFile.getMapDir(basedir, encodedRegionName, colFamily));
    fs.mkdirs(HStoreFile.getInfoDir(basedir, encodedRegionName, colFamily));
  }
}
