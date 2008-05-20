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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.BloomFilterDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.onelab.filter.BloomFilter;
import org.onelab.filter.CountingBloomFilter;
import org.onelab.filter.Filter;
import org.onelab.filter.RetouchedBloomFilter;

/**
 * HStore maintains a bunch of data files.  It is responsible for maintaining 
 * the memory/file hierarchy and for periodic flushes to disk and compacting 
 * edits to the file.
 *
 * Locking and transactions are handled at a higher level.  This API should not 
 * be called directly by any writer, but rather by an HRegion manager.
 */
public class HStore implements HConstants {
  static final Log LOG = LogFactory.getLog(HStore.class);

  /*
   * Regex that will work for straight filenames and for reference names.
   * If reference, then the regex has more than just one group.  Group 1 is
   * this files id.  Group 2 the referenced region name, etc.
   */
  private static final Pattern REF_NAME_PARSER =
    Pattern.compile("^(\\d+)(?:\\.(.+))?$");
  
  private static final String BLOOMFILTER_FILE_NAME = "filter";

  protected final Memcache memcache;
  private final Path basedir;
  private final HRegionInfo info;
  private final HColumnDescriptor family;
  private final SequenceFile.CompressionType compression;
  final FileSystem fs;
  private final HBaseConfiguration conf;
  private final Path filterDir;
  final Filter bloomFilter;
  protected long ttl;

  private final long desiredMaxFileSize;
  private volatile long storeSize;

  private final Integer flushLock = new Integer(0);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final byte [] storeName;
  private final String storeNameStr;

  /*
   * Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  private final SortedMap<Long, HStoreFile> storefiles =
    Collections.synchronizedSortedMap(new TreeMap<Long, HStoreFile>());
  
  /*
   * Sorted Map of readers keyed by sequence id (Most recent should be last in
   * in list).
   */
  private final SortedMap<Long, MapFile.Reader> readers =
    new TreeMap<Long, MapFile.Reader>();

  // The most-recent log-seq-ID that's present.  The most-recent such ID means
  // we can ignore all log messages up to and including that ID (because they're
  // already reflected in the TreeMaps).
  private volatile long maxSeqId;
  
  private final Path compactionDir;
  private final Integer compactLock = new Integer(0);
  private final int compactionThreshold;
  private final Set<ChangedReadersObserver> changedReaderObservers =
    Collections.synchronizedSet(new HashSet<ChangedReadersObserver>());

  /**
   * An HStore is a set of zero or more MapFiles, which stretch backwards over 
   * time.  A given HStore is responsible for a certain set of columns for a
   * row in the HRegion.
   *
   * <p>The HRegion starts writing to its set of HStores when the HRegion's 
   * memcache is flushed.  This results in a round of new MapFiles, one for
   * each HStore.
   *
   * <p>There's no reason to consider append-logging at this level; all logging 
   * and locking is handled at the HRegion level.  HStore just provides
   * services to manage sets of MapFiles.  One of the most important of those
   * services is MapFile-compaction services.
   *
   * <p>The only thing having to do with logs that HStore needs to deal with is
   * the reconstructionLog.  This is a segment of an HRegion's log that might
   * NOT be present upon startup.  If the param is NULL, there's nothing to do.
   * If the param is non-NULL, we need to process the log to reconstruct
   * a TreeMap that might not have been written to disk before the process
   * died.
   *
   * <p>It's assumed that after this constructor returns, the reconstructionLog
   * file will be deleted (by whoever has instantiated the HStore).
   *
   * @param basedir qualified path under which the region directory lives
   * @param info HRegionInfo for this region
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param reconstructionLog existing log file to apply if any
   * @param conf configuration object
   * @param reporter Call on a period so hosting server can report we're
   * making progress to master -- otherwise master might think region deploy
   * failed.  Can be null.
   * @throws IOException
   */
  HStore(Path basedir, HRegionInfo info, HColumnDescriptor family,
      FileSystem fs, Path reconstructionLog, HBaseConfiguration conf,
      final Progressable reporter)
  throws IOException {  
    this.basedir = basedir;
    this.info = info;
    this.family = family;
    this.fs = fs;
    this.conf = conf;
    this.ttl = family.getTimeToLive();
    if (ttl != HConstants.FOREVER)
      this.ttl *= 1000;
    this.memcache = new Memcache(this.ttl);
    this.compactionDir = HRegion.getCompactionDir(basedir);
    this.storeName = Bytes.toBytes(this.info.getEncodedName() + "/" +
      Bytes.toString(this.family.getName()));
    this.storeNameStr = Bytes.toString(this.storeName);
    
    // By default, we compact if an HStore has more than
    // MIN_COMMITS_FOR_COMPACTION map files
    this.compactionThreshold =
      conf.getInt("hbase.hstore.compactionThreshold", 3);
    
    // By default we split region if a file > DEFAULT_MAX_FILE_SIZE.
    this.desiredMaxFileSize =
      conf.getLong("hbase.hregion.max.filesize", DEFAULT_MAX_FILE_SIZE);
    this.storeSize = 0L;

    if (family.getCompression() == HColumnDescriptor.CompressionType.BLOCK) {
      this.compression = SequenceFile.CompressionType.BLOCK;
    } else if (family.getCompression() ==
      HColumnDescriptor.CompressionType.RECORD) {
      this.compression = SequenceFile.CompressionType.RECORD;
    } else {
      this.compression = SequenceFile.CompressionType.NONE;
    }
    
    Path mapdir = HStoreFile.getMapDir(basedir, info.getEncodedName(),
        family.getName());
    if (!fs.exists(mapdir)) {
      fs.mkdirs(mapdir);
    }
    Path infodir = HStoreFile.getInfoDir(basedir, info.getEncodedName(),
        family.getName());
    if (!fs.exists(infodir)) {
      fs.mkdirs(infodir);
    }
    
    if(family.getBloomFilter() == null) {
      this.filterDir = null;
      this.bloomFilter = null;
    } else {
      this.filterDir = HStoreFile.getFilterDir(basedir, info.getEncodedName(),
          family.getName());
      if (!fs.exists(filterDir)) {
        fs.mkdirs(filterDir);
      }
      this.bloomFilter = loadOrCreateBloomFilter();
    }

    // Go through the 'mapdir' and 'infodir' together, make sure that all 
    // MapFiles are in a reliable state.  Every entry in 'mapdir' must have a 
    // corresponding one in 'loginfodir'. Without a corresponding log info
    // file, the entry in 'mapdir' must be deleted.
    // loadHStoreFiles also computes the max sequence id internally.
    this.maxSeqId = -1L;
    this.storefiles.putAll(loadHStoreFiles(infodir, mapdir));
    if (LOG.isDebugEnabled() && this.storefiles.size() > 0) {
      LOG.debug("Loaded " + this.storefiles.size() + " file(s) in hstore " +
        Bytes.toString(this.storeName) + ", max sequence id " + this.maxSeqId);
    }
    
    try {
      doReconstructionLog(reconstructionLog, maxSeqId, reporter);
    } catch (IOException e) {
      // Presume we got here because of some HDFS issue or because of a lack of
      // HADOOP-1700; for now keep going but this is probably not what we want
      // long term.  If we got here there has been data-loss
      LOG.warn("Exception processing reconstruction log " + reconstructionLog +
        " opening " + Bytes.toString(this.storeName) +
        " -- continuing.  Probably DATA LOSS!", e);
    }

    // Finally, start up all the map readers! (There could be more than one
    // since we haven't compacted yet.)
    boolean first = true;
    for(Map.Entry<Long, HStoreFile> e: this.storefiles.entrySet()) {
      if (first) {
        // Use a block cache (if configured) for the first reader only
        // so as to control memory usage.
        this.readers.put(e.getKey(),
            e.getValue().getReader(this.fs, this.bloomFilter,
                family.isBlockCacheEnabled()));
        first = false;
      } else {
        this.readers.put(e.getKey(),
            e.getValue().getReader(this.fs, this.bloomFilter));
      }
    }
  }

  HColumnDescriptor getFamily() {
    return this.family;
  }
  
  long getMaxSequenceId() {
    return this.maxSeqId;
  }
  
  /*
   * Read the reconstructionLog to see whether we need to build a brand-new 
   * MapFile out of non-flushed log entries.  
   *
   * We can ignore any log message that has a sequence ID that's equal to or 
   * lower than maxSeqID.  (Because we know such log messages are already 
   * reflected in the MapFiles.)
   */
  private void doReconstructionLog(final Path reconstructionLog,
    final long maxSeqID, final Progressable reporter)
  throws UnsupportedEncodingException, IOException {
    if (reconstructionLog == null || !fs.exists(reconstructionLog)) {
      // Nothing to do.
      return;
    }
    // Check its not empty.
    FileStatus[] stats = fs.listStatus(reconstructionLog);
    if (stats == null || stats.length == 0) {
      LOG.warn("Passed reconstruction log " + reconstructionLog + " is zero-length");
      return;
    }
    long maxSeqIdInLog = -1;
    TreeMap<HStoreKey, byte []> reconstructedCache =
      new TreeMap<HStoreKey, byte []>();
      
    SequenceFile.Reader logReader = new SequenceFile.Reader(this.fs,
        reconstructionLog, this.conf);
    
    try {
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      long skippedEdits = 0;
      long editsCount = 0;
      // How many edits to apply before we send a progress report.
      int reportInterval = this.conf.getInt("hbase.hstore.report.interval.edits", 2000);
      while (logReader.next(key, val)) {
        maxSeqIdInLog = Math.max(maxSeqIdInLog, key.getLogSeqNum());
        if (key.getLogSeqNum() <= maxSeqID) {
          skippedEdits++;
          continue;
        }
        // Check this edit is for me. Also, guard against writing
        // METACOLUMN info such as HBASE::CACHEFLUSH entries
        byte [] column = val.getColumn();
        if (Bytes.equals(column, HLog.METACOLUMN)
            || !Bytes.equals(key.getRegionName(), info.getRegionName())
            || !HStoreKey.matchingFamily(family.getName(), column)) {
          continue;
        }
        HStoreKey k = new HStoreKey(key.getRow(), column, val.getTimestamp());
        reconstructedCache.put(k, val.getVal());
        editsCount++;
        // Every 2k edits, tell the reporter we're making progress.
        // Have seen 60k edits taking 3minutes to complete.
        if (reporter != null && (editsCount % reportInterval) == 0) {
          reporter.progress();
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Applied " + editsCount + ", skipped " + skippedEdits +
          " because sequence id <= " + maxSeqID);
      }
    } finally {
      logReader.close();
    }
    
    if (reconstructedCache.size() > 0) {
      // We create a "virtual flush" at maxSeqIdInLog+1.
      if (LOG.isDebugEnabled()) {
        LOG.debug("flushing reconstructionCache");
      }
      internalFlushCache(reconstructedCache, maxSeqIdInLog + 1);
    }
  }
  
  /*
   * Creates a series of HStoreFiles loaded from the given directory.
   * There must be a matching 'mapdir' and 'loginfo' pair of files.
   * If only one exists, we'll delete it.
   *
   * @param infodir qualified path for info file directory
   * @param mapdir qualified path for map file directory
   * @throws IOException
   */
  private SortedMap<Long, HStoreFile> loadHStoreFiles(Path infodir, Path mapdir)
  throws IOException {
    // Look first at info files.  If a reference, these contain info we need
    // to create the HStoreFile.
    FileStatus infofiles[] = fs.listStatus(infodir);
    SortedMap<Long, HStoreFile> results = new TreeMap<Long, HStoreFile>();
    ArrayList<Path> mapfiles = new ArrayList<Path>(infofiles.length);
    for (int i = 0; i < infofiles.length; i++) {
      Path p = infofiles[i].getPath();
      Matcher m = REF_NAME_PARSER.matcher(p.getName());
      /*
       *  *  *  *  *  N O T E  *  *  *  *  *
       *  
       *  We call isReference(Path, Matcher) here because it calls
       *  Matcher.matches() which must be called before Matcher.group(int)
       *  and we don't want to call Matcher.matches() twice.
       *  
       *  *  *  *  *  N O T E  *  *  *  *  *
       */
      boolean isReference = isReference(p, m);
      long fid = Long.parseLong(m.group(1));
      
      HStoreFile curfile = null;
      HStoreFile.Reference reference = null;
      if (isReference) {
        reference = HStoreFile.readSplitInfo(p, fs);
      }
      curfile = new HStoreFile(conf, fs, basedir, info.getEncodedName(),
        family.getName(), fid, reference);
      storeSize += curfile.length();
      long storeSeqId = -1;
      try {
        storeSeqId = curfile.loadInfo(fs);
        if (storeSeqId > this.maxSeqId) {
          this.maxSeqId = storeSeqId;
        }
      } catch (IOException e) {
        // If the HSTORE_LOGINFOFILE doesn't contain a number, just ignore it.
        // That means it was built prior to the previous run of HStore, and so
        // it cannot contain any updates also contained in the log.
        LOG.info("HSTORE_LOGINFOFILE " + curfile +
          " does not contain a sequence number - ignoring");
      }

      Path mapfile = curfile.getMapFilePath();
      if (!fs.exists(mapfile)) {
        fs.delete(curfile.getInfoFilePath());
        LOG.warn("Mapfile " + mapfile.toString() + " does not exist. " +
          "Cleaned up info file.  Continuing...");
        continue;
      }
      
      // TODO: Confirm referent exists.
      
      // Found map and sympathetic info file.  Add this hstorefile to result.
      if (LOG.isDebugEnabled()) {
        LOG.debug("loaded " + FSUtils.getPath(p) + ", isReference=" +
          isReference + ", sequence id=" + storeSeqId);
      }
      results.put(Long.valueOf(storeSeqId), curfile);
      // Keep list of sympathetic data mapfiles for cleaning info dir in next
      // section.  Make sure path is fully qualified for compare.
      mapfiles.add(mapfile);
    }
    
    // List paths by experience returns fully qualified names -- at least when
    // running on a mini hdfs cluster.
    FileStatus datfiles[] = fs.listStatus(mapdir);
    for (int i = 0; i < datfiles.length; i++) {
      Path p = datfiles[i].getPath();
      // If does not have sympathetic info file, delete.
      if (!mapfiles.contains(fs.makeQualified(p))) {
        fs.delete(p);
      }
    }
    return results;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Called by constructor if a bloom filter is enabled for this column family.
   * If the HStore already exists, it will read in the bloom filter saved
   * previously. Otherwise, it will create a new bloom filter.
   */
  private Filter loadOrCreateBloomFilter() throws IOException {
    Path filterFile = new Path(filterDir, BLOOMFILTER_FILE_NAME);
    Filter bloomFilter = null;
    if(fs.exists(filterFile)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("loading bloom filter for " + this.storeNameStr);
      }
      
      BloomFilterDescriptor.BloomFilterType type =
        family.getBloomFilter().getType();

      switch(type) {
      
      case BLOOMFILTER:
        bloomFilter = new BloomFilter();
        break;
        
      case COUNTING_BLOOMFILTER:
        bloomFilter = new CountingBloomFilter();
        break;
        
      case RETOUCHED_BLOOMFILTER:
        bloomFilter = new RetouchedBloomFilter();
        break;
      
      default:
        throw new IllegalArgumentException("unknown bloom filter type: " +
            type);
      }
      FSDataInputStream in = fs.open(filterFile);
      try {
        bloomFilter.readFields(in);
      } finally {
        fs.close();
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("creating bloom filter for " + this.storeNameStr);
      }

      BloomFilterDescriptor.BloomFilterType type =
        family.getBloomFilter().getType();

      switch(type) {
      
      case BLOOMFILTER:
        bloomFilter = new BloomFilter(family.getBloomFilter().getVectorSize(),
            family.getBloomFilter().getNbHash());
        break;
        
      case COUNTING_BLOOMFILTER:
        bloomFilter =
          new CountingBloomFilter(family.getBloomFilter().getVectorSize(),
            family.getBloomFilter().getNbHash());
        break;
        
      case RETOUCHED_BLOOMFILTER:
        bloomFilter =
          new RetouchedBloomFilter(family.getBloomFilter().getVectorSize(),
            family.getBloomFilter().getNbHash());
      }
    }
    return bloomFilter;
  }

  /**
   * Flushes bloom filter to disk
   * 
   * @throws IOException
   */
  private void flushBloomFilter() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushing bloom filter for " + this.storeNameStr);
    }
    FSDataOutputStream out =
      fs.create(new Path(filterDir, BLOOMFILTER_FILE_NAME));
    try {
      bloomFilter.write(out);
    } finally {
      out.close();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("flushed bloom filter for " + this.storeNameStr);
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // End bloom filters
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Adds a value to the memcache
   * 
   * @param key
   * @param value
   */
  void add(HStoreKey key, byte[] value) {
    lock.readLock().lock();
    try {
      this.memcache.add(key, value);
    } finally {
      lock.readLock().unlock();
    }
  }
  
  /**
   * Close all the MapFile readers
   * 
   * We don't need to worry about subsequent requests because the HRegion holds
   * a write lock that will prevent any more reads or writes.
   * 
   * @throws IOException
   */
  List<HStoreFile> close() throws IOException {
    ArrayList<HStoreFile> result = null;
    this.lock.writeLock().lock();
    try {
      for (MapFile.Reader reader: this.readers.values()) {
        reader.close();
      }
      synchronized (this.storefiles) {
        result = new ArrayList<HStoreFile>(storefiles.values());
      }
      LOG.debug("closed " + this.storeNameStr);
      return result;
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  // Flush changes to disk
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Snapshot this stores memcache.  Call before running
   * {@link #flushCache(long)} so it has some work to do.
   */
  void snapshot() {
    this.memcache.snapshot();
  }
  
  /**
   * Write out current snapshot.  Presumes {@link #snapshot()} has been called
   * previously.
   * @param logCacheFlushId flush sequence number
   * @return count of bytes flushed
   * @throws IOException
   */
  long flushCache(final long logCacheFlushId) throws IOException {
    // Get the snapshot to flush.  Presumes that a call to
    // this.memcache.snapshot() has happened earlier up in the chain.
    SortedMap<HStoreKey, byte []> cache = this.memcache.getSnapshot();
    long flushed = internalFlushCache(cache, logCacheFlushId);
    // If an exception happens flushing, we let it out without clearing
    // the memcache snapshot.  The old snapshot will be returned when we say
    // 'snapshot', the next time flush comes around.
    this.memcache.clearSnapshot(cache);
    return flushed;
  }
  
  private long internalFlushCache(SortedMap<HStoreKey, byte []> cache,
      long logCacheFlushId) throws IOException {
    long flushed = 0;
    // Don't flush if there are no entries.
    if (cache.size() == 0) {
      return flushed;
    }
    
    // TODO:  We can fail in the below block before we complete adding this
    // flush to list of store files.  Add cleanup of anything put on filesystem
    // if we fail.
    synchronized(flushLock) {
      long now = System.currentTimeMillis();
      // A. Write the Maps out to the disk
      HStoreFile flushedFile = new HStoreFile(conf, fs, basedir,
        info.getEncodedName(),  family.getName(), -1L, null);
      MapFile.Writer out = flushedFile.getWriter(this.fs, this.compression,
        this.bloomFilter);
      
      // Here we tried picking up an existing HStoreFile from disk and
      // interlacing the memcache flush compacting as we go.  The notion was
      // that interlacing would take as long as a pure flush with the added
      // benefit of having one less file in the store.  Experiments showed that
      // it takes two to three times the amount of time flushing -- more column
      // families makes it so the two timings come closer together -- but it
      // also complicates the flush. The code was removed.  Needed work picking
      // which file to interlace (favor references first, etc.)
      //
      // Related, looks like 'merging compactions' in BigTable paper interlaces
      // a memcache flush.  We don't.
      int entries = 0;
      try {
        for (Map.Entry<HStoreKey, byte []> es: cache.entrySet()) {
          HStoreKey curkey = es.getKey();
          byte[] bytes = es.getValue();
          if (HStoreKey.matchingFamily(this.family.getName(), curkey.getColumn())) {
            if (ttl == HConstants.FOREVER ||
                  now < curkey.getTimestamp() + ttl) {
              entries++;
              out.append(curkey, new ImmutableBytesWritable(bytes));
              flushed += HRegion.getEntrySize(curkey, bytes);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("internalFlushCache: " + curkey +
                  ": expired, skipped");
              }
            }
          }
        }
      } finally {
        out.close();
      }
      long newStoreSize = flushedFile.length();
      storeSize += newStoreSize;

      // B. Write out the log sequence number that corresponds to this output
      // MapFile.  The MapFile is current up to and including the log seq num.
      flushedFile.writeInfo(fs, logCacheFlushId);
      
      // C. Flush the bloom filter if any
      if (bloomFilter != null) {
        flushBloomFilter();
      }

      // D. Finally, make the new MapFile available.
      updateReaders(logCacheFlushId, flushedFile);
      if(LOG.isDebugEnabled()) {
        LOG.debug("Added " + FSUtils.getPath(flushedFile.getMapFilePath()) +
          " with " + entries +
          " entries, sequence id " + logCacheFlushId + ", data size " +
          StringUtils.humanReadableInt(flushed) + ", file size " +
          StringUtils.humanReadableInt(newStoreSize));
      }
    }
    return flushed;
  }
  
  /*
   * Change readers adding into place the Reader produced by this new flush.
   * @param logCacheFlushId
   * @param flushedFile
   * @throws IOException
   */
  private void updateReaders(final long logCacheFlushId,
      final HStoreFile flushedFile)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      Long flushid = Long.valueOf(logCacheFlushId);
      // Open the map file reader.
      this.readers.put(flushid,
        flushedFile.getReader(this.fs, this.bloomFilter));
      this.storefiles.put(flushid, flushedFile);
      // Tell listeners of the change in readers.
      notifyChangedReadersObservers();
    } finally {
      this.lock.writeLock().unlock();
    }
  }
  
  /*
   * Notify all observers that set of Readers has changed.
   * @throws IOException
   */
  private void notifyChangedReadersObservers() throws IOException {
    synchronized (this.changedReaderObservers) {
      for (ChangedReadersObserver o: this.changedReaderObservers) {
        o.updateReaders();
      }
    }
  }
  
  /*
   * @param o Observer who wants to know about changes in set of Readers
   */
  void addChangedReaderObserver(ChangedReadersObserver o) {
    this.changedReaderObservers.add(o);
  }
  
  /*
   * @param o Observer no longer interested in changes in set of Readers.
   */
  void deleteChangedReaderObserver(ChangedReadersObserver o) {
    if (!this.changedReaderObservers.remove(o)) {
      LOG.warn("Not in set" + o);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Compaction
  //////////////////////////////////////////////////////////////////////////////

  /*
   * @param files
   * @return True if any of the files in <code>files</code> are References.
   */
  private boolean hasReferences(Collection<HStoreFile> files) {
    if (files != null && files.size() > 0) {
      for (HStoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }
  
  /**
   * Compact the back-HStores.  This method may take some time, so the calling 
   * thread must be able to block for long periods.
   * 
   * <p>During this time, the HStore can work as usual, getting values from
   * MapFiles and writing new MapFiles from the Memcache.
   * 
   * Existing MapFiles are not destroyed until the new compacted TreeMap is 
   * completely written-out to disk.
   *
   * The compactLock prevents multiple simultaneous compactions.
   * The structureLock prevents us from interfering with other write operations.
   * 
   * We don't want to hold the structureLock for the whole time, as a compact() 
   * can be lengthy and we want to allow cache-flushes during this period.
   * 
   * @param force True to force a compaction regardless of thresholds (Needed
   * by merge).
   * @return mid key if a split is needed, null otherwise
   * @throws IOException
   */
  byte [] compact(final boolean force) throws IOException {
    synchronized (compactLock) {
      long maxId = -1;
      List<HStoreFile> filesToCompact = null;
      synchronized (storefiles) {
        if (this.storefiles.size() <= 0) {
          return null;
        }
        filesToCompact = new ArrayList<HStoreFile>(this.storefiles.values());
        if (!force && !hasReferences(filesToCompact) &&
             filesToCompact.size() < compactionThreshold) {
          return checkSplit();
        }
        if (!fs.exists(compactionDir) && !fs.mkdirs(compactionDir)) {
          LOG.warn("Mkdir on " + compactionDir.toString() + " failed");
          return checkSplit();
        }

        // The max-sequenceID in any of the to-be-compacted TreeMaps is the 
        // last key of storefiles.
        maxId = this.storefiles.lastKey().longValue();
      }
      // Storefiles are keyed by sequence id. The oldest file comes first.
      // We need to return out of here a List that has the newest file first.
      Collections.reverse(filesToCompact);

      // Step through them, writing to the brand-new MapFile
      HStoreFile compactedOutputFile = new HStoreFile(conf, fs, 
          this.compactionDir, info.getEncodedName(), family.getName(),
          -1L, null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("started compaction of " + filesToCompact.size() +
          " files " + filesToCompact.toString() + " into " +
          FSUtils.getPath(compactedOutputFile.getMapFilePath()));
      }
      MapFile.Writer compactedOut = compactedOutputFile.getWriter(this.fs,
        this.compression, this.bloomFilter);
      try {
        compactHStoreFiles(compactedOut, filesToCompact);
      } finally {
        compactedOut.close();
      }

      // Now, write out an HSTORE_LOGINFOFILE for the brand-new TreeMap.
      compactedOutputFile.writeInfo(fs, maxId);

      // Move the compaction into place.
      completeCompaction(filesToCompact, compactedOutputFile);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Completed compaction of " + this.storeNameStr +
            " store size is " + StringUtils.humanReadableInt(storeSize));
      }
    }
    return checkSplit();
  }
  
  /*
   * Compact passed <code>toCompactFiles</code> into <code>compactedOut</code>.
   * We create a new set of MapFile.Reader objects so we don't screw up the
   * caching associated with the currently-loaded ones. Our iteration-based
   * access pattern is practically designed to ruin the cache.
   * 
   * We work by opening a single MapFile.Reader for each file, and iterating
   * through them in parallel. We always increment the lowest-ranked one.
   * Updates to a single row/column will appear ranked by timestamp. This allows
   * us to throw out deleted values or obsolete versions. @param compactedOut
   * @param toCompactFiles @throws IOException
   */
  private void compactHStoreFiles(final MapFile.Writer compactedOut,
      final List<HStoreFile> toCompactFiles) throws IOException {
    
    int size = toCompactFiles.size();
    CompactionReader[] rdrs = new CompactionReader[size];
    int index = 0;
    for (HStoreFile hsf: toCompactFiles) {
      try {
        rdrs[index++] =
          new MapFileCompactionReader(hsf.getReader(fs, bloomFilter));
      } catch (IOException e) {
        // Add info about which file threw exception. It may not be in the
        // exception message so output a message here where we know the
        // culprit.
        LOG.warn("Failed with " + e.toString() + ": " + hsf.toString());
        closeCompactionReaders(rdrs);
        throw e;
      }
    }
    try {
      HStoreKey[] keys = new HStoreKey[rdrs.length];
      ImmutableBytesWritable[] vals = new ImmutableBytesWritable[rdrs.length];
      boolean[] done = new boolean[rdrs.length];
      for(int i = 0; i < rdrs.length; i++) {
        keys[i] = new HStoreKey();
        vals[i] = new ImmutableBytesWritable();
        done[i] = false;
      }

      // Now, advance through the readers in order.  This will have the
      // effect of a run-time sort of the entire dataset.
      int numDone = 0;
      for(int i = 0; i < rdrs.length; i++) {
        rdrs[i].reset();
        done[i] = ! rdrs[i].next(keys[i], vals[i]);
        if(done[i]) {
          numDone++;
        }
      }

      long now = System.currentTimeMillis();
      int timesSeen = 0;
      byte [] lastRow = null;
      byte [] lastColumn = null;
      // Map of a row deletes keyed by column with a list of timestamps for value
      Map<byte [], List<Long>> deletes = null;
      while (numDone < done.length) {
        // Find the reader with the smallest key.  If two files have same key
        // but different values -- i.e. one is delete and other is non-delete
        // value -- we will find the first, the one that was written later and
        // therefore the one whose value should make it out to the compacted
        // store file.
        int smallestKey = -1;
        for(int i = 0; i < rdrs.length; i++) {
          if(done[i]) {
            continue;
          }
          if(smallestKey < 0) {
            smallestKey = i;
          } else {
            if(keys[i].compareTo(keys[smallestKey]) < 0) {
              smallestKey = i;
            }
          }
        }

        // Reflect the current key/val in the output
        HStoreKey sk = keys[smallestKey];
        if (Bytes.equals(lastRow, sk.getRow())
            && Bytes.equals(lastColumn, sk.getColumn())) {
          timesSeen++;
        } else {
          timesSeen = 0;
          // We are on to a new row.  Create a new deletes list.
          deletes = new TreeMap<byte [], List<Long>>(Bytes.BYTES_COMPARATOR);
        }

        byte [] value = (vals[smallestKey] == null)?
          null: vals[smallestKey].get();
        if (!isDeleted(sk, value, false, deletes) &&
            timesSeen <= family.getMaxVersions()) {
          // Keep old versions until we have maxVersions worth.
          // Then just skip them.
          if (sk.getRow().length != 0 && sk.getColumn().length != 0) {
            // Only write out objects which have a non-zero length key and
            // value
            if (ttl == HConstants.FOREVER || now < sk.getTimestamp() + ttl) {
              compactedOut.append(sk, vals[smallestKey]);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("compactHStoreFiles: " + sk + ": expired, deleted");
              }
            }
          }
        }

        // Update last-seen items
        lastRow = sk.getRow();
        lastColumn = sk.getColumn();

        // Advance the smallest key.  If that reader's all finished, then 
        // mark it as done.
        if(!rdrs[smallestKey].next(keys[smallestKey],
            vals[smallestKey])) {
          done[smallestKey] = true;
          rdrs[smallestKey].close();
          rdrs[smallestKey] = null;
          numDone++;
        }
      }
    } finally {
      closeCompactionReaders(rdrs);
    }
  }
  
  private void closeCompactionReaders(final CompactionReader [] rdrs) {
    for (int i = 0; i < rdrs.length; i++) {
      if (rdrs[i] != null) {
        try {
          rdrs[i].close();
        } catch (IOException e) {
          LOG.warn("Exception closing reader for " + this.storeNameStr, e);
        }
      }
    }
  }

  /*
   * Check if this is cell is deleted.
   * If a memcache and a deletes, check key does not have an entry filled.
   * Otherwise, check value is not the <code>HGlobals.deleteBytes</code> value.
   * If passed value IS deleteBytes, then it is added to the passed
   * deletes map.
   * @param hsk
   * @param value
   * @param checkMemcache true if the memcache should be consulted
   * @param deletes Map keyed by column with a value of timestamp. Can be null.
   * If non-null and passed value is HGlobals.deleteBytes, then we add to this
   * map.
   * @return True if this is a deleted cell.  Adds the passed deletes map if
   * passed value is HGlobals.deleteBytes.
  */
  private boolean isDeleted(final HStoreKey hsk, final byte [] value,
      final boolean checkMemcache, final Map<byte [], List<Long>> deletes) {
    if (checkMemcache && memcache.isDeleted(hsk)) {
      return true;
    }
    List<Long> timestamps =
      (deletes == null) ? null: deletes.get(hsk.getColumn());
    if (timestamps != null &&
        timestamps.contains(Long.valueOf(hsk.getTimestamp()))) {
      return true;
    }
    if (value == null) {
      // If a null value, shouldn't be in here.  Mark it as deleted cell.
      return true;
    }
    if (!HLogEdit.isDeleted(value)) {
      return false;
    }
    // Cell has delete value.  Save it into deletes.
    if (deletes != null) {
      if (timestamps == null) {
        timestamps = new ArrayList<Long>();
        deletes.put(hsk.getColumn(), timestamps);
      }
      // We know its not already in the deletes array else we'd have returned
      // earlier so no need to test if timestamps already has this value.
      timestamps.add(Long.valueOf(hsk.getTimestamp()));
    }
    return true;
  }
  
  /*
   * It's assumed that the compactLock  will be acquired prior to calling this 
   * method!  Otherwise, it is not thread-safe!
   *
   * It works by processing a compaction that's been written to disk.
   * 
   * <p>It is usually invoked at the end of a compaction, but might also be
   * invoked at HStore startup, if the prior execution died midway through.
   * 
   * <p>Moving the compacted TreeMap into place means:
   * <pre>
   * 1) Moving the new compacted MapFile into place
   * 2) Unload all replaced MapFiles, close and collect list to delete.
   * 3) Loading the new TreeMap.
   * 4) Compute new store size
   * </pre>
   * 
   * @param compactedFiles list of files that were compacted
   * @param compactedFile HStoreFile that is the result of the compaction
   * @throws IOException
   */
  private void completeCompaction(final List<HStoreFile> compactedFiles,
    final HStoreFile compactedFile)
  throws IOException {
    this.lock.writeLock().lock();
    try {
      // 1. Moving the new MapFile into place.
      HStoreFile finalCompactedFile = new HStoreFile(conf, fs, basedir,
        info.getEncodedName(), family.getName(), -1, null);
      if (LOG.isDebugEnabled()) {
        LOG.debug("moving " + FSUtils.getPath(compactedFile.getMapFilePath()) +
          " to " + FSUtils.getPath(finalCompactedFile.getMapFilePath()));
      }
      if (!compactedFile.rename(this.fs, finalCompactedFile)) {
        LOG.error("Failed move of compacted file " +
          finalCompactedFile.getMapFilePath().toString());
        return;
      }

      // 2. Unload all replaced MapFiles, close and collect list to delete.
      synchronized (storefiles) {
        Map<Long, HStoreFile> toDelete = new HashMap<Long, HStoreFile>();
        for (Map.Entry<Long, HStoreFile> e : this.storefiles.entrySet()) {
          if (!compactedFiles.contains(e.getValue())) {
            continue;
          }
          Long key = e.getKey();
          MapFile.Reader reader = this.readers.remove(key);
          if (reader != null) {
            reader.close();
          }
          toDelete.put(key, e.getValue());
        }

        try {
          // 3. Loading the new TreeMap.
          // Change this.storefiles so it reflects new state but do not
          // delete old store files until we have sent out notification of
          // change in case old files are still being accessed by outstanding
          // scanners.
          for (Long key : toDelete.keySet()) {
            this.storefiles.remove(key);
          }
          // Add new compacted Reader and store file.
          Long orderVal = Long.valueOf(finalCompactedFile.loadInfo(fs));
          this.readers.put(orderVal,
          // Use a block cache (if configured) for this reader since
              // it is the only one.
              finalCompactedFile.getReader(this.fs, this.bloomFilter, family
                  .isBlockCacheEnabled()));
          this.storefiles.put(orderVal, finalCompactedFile);
          // Tell observers that list of Readers has changed.
          notifyChangedReadersObservers();
          // Finally, delete old store files.
          for (HStoreFile hsf : toDelete.values()) {
            hsf.delete();
          }
        } catch (IOException e) {
          e = RemoteExceptionHandler.checkIOException(e);
          LOG.error("Failed replacing compacted files for " +
            this.storeNameStr +
            ". Compacted file is " + finalCompactedFile.toString() +
            ".  Files replaced are " + compactedFiles.toString() +
            " some of which may have been already removed", e);
        }
        // 4. Compute new store size
        storeSize = 0L;
        for (HStoreFile hsf : storefiles.values()) {
          storeSize += hsf.length();
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Accessors.
  // (This is the only section that is directly useful!)
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Return all the available columns for the given key.  The key indicates a 
   * row and timestamp, but not a column name.
   *
   * The returned object should map column names to Cells.
   */
  void getFull(HStoreKey key, final Set<byte []> columns,
      Map<byte [], Cell> results)
  throws IOException {
    Map<byte [], Long> deletes =
      new TreeMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

    // if the key is null, we're not even looking for anything. return.
    if (key == null) {
      return;
    }
    
    this.lock.readLock().lock();
    
    // get from the memcache first.
    memcache.getFull(key, columns, deletes, results);
    
    try {
      MapFile.Reader[] maparray = getReaders();
      
      // examine each mapfile
      for (int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        
        // synchronize on the map so that no one else iterates it at the same 
        // time
        getFullFromMapFile(map, key, columns, deletes, results);
      }
      
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  private void getFullFromMapFile(MapFile.Reader map, HStoreKey key, 
    Set<byte []> columns, Map<byte [], Long> deletes, Map<byte [], Cell> results) 
  throws IOException {
    synchronized(map) {
      long now = System.currentTimeMillis();

      // seek back to the beginning
      map.reset();
      
      // seek to the closest key that should match the row we're looking for
      ImmutableBytesWritable readval = new ImmutableBytesWritable();
      HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
      if (readkey == null) {
        return;
      }
      do {
        byte [] readcol = readkey.getColumn();
        
        // if we're looking for this column (or all of them), and there isn't 
        // already a value for this column in the results map, and the key we 
        // just read matches, then we'll consider it
        if ((columns == null || columns.contains(readcol)) 
          && !results.containsKey(readcol)
          && key.matchesWithoutColumn(readkey)) {
          // if the value of the cell we're looking at right now is a delete, 
          // we need to treat it differently
          if(HLogEdit.isDeleted(readval.get())) {
            // if it's not already recorded as a delete or recorded with a more
            // recent delete timestamp, record it for later
            if (!deletes.containsKey(readcol) 
              || deletes.get(readcol).longValue() < readkey.getTimestamp()) {
              deletes.put(readcol, readkey.getTimestamp());              
            }
          } else if (!(deletes.containsKey(readcol) 
            && deletes.get(readcol).longValue() >= readkey.getTimestamp()) ) {
            // So the cell itself isn't a delete, but there may be a delete 
            // pending from earlier in our search. Only record this result if
            // there aren't any pending deletes.
            if (!(deletes.containsKey(readcol) &&
                deletes.get(readcol).longValue() >= readkey.getTimestamp())) {
              if (ttl == HConstants.FOREVER ||
                      now < readkey.getTimestamp() + ttl) {
                results.put(readcol, 
                  new Cell(readval.get(), readkey.getTimestamp()));
                // need to reinstantiate the readval so we can reuse it, 
                // otherwise next iteration will destroy our result
                readval = new ImmutableBytesWritable();
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("getFullFromMapFile: " + readkey + ": expired, skipped");
                }
              }
            }
          }
        } else if (Bytes.compareTo(key.getRow(), readkey.getRow()) < 0) {
          // if we've crossed into the next row, then we can just stop 
          // iterating
          break;
        }
        
      } while(map.next(readkey, readval));
    }
  }
  
  MapFile.Reader [] getReaders() {
    return this.readers.values().
      toArray(new MapFile.Reader[this.readers.size()]);
  }

  /**
   * Get the value for the indicated HStoreKey.  Grab the target value and the 
   * previous 'numVersions-1' values, as well.
   *
   * Use {@link HConstants.ALL_VERSIONS} to retrieve all versions.
   * @param key
   * @param numVersions Number of versions to fetch.  Must be > 0.
   * @return values for the specified versions
   * @throws IOException
   */
  Cell[] get(HStoreKey key, int numVersions) throws IOException {
    if (numVersions <= 0) {
      throw new IllegalArgumentException("Number of versions must be > 0");
    }
    
    this.lock.readLock().lock();
    long now = System.currentTimeMillis();
    try {
      // Check the memcache
      List<Cell> results = this.memcache.get(key, numVersions);
      // If we got sufficient versions from memcache, return. 
      if (results.size() == numVersions) {
        return results.toArray(new Cell[results.size()]);
      }

      // Keep a list of deleted cell keys.  We need this because as we go through
      // the store files, the cell with the delete marker may be in one file and
      // the old non-delete cell value in a later store file. If we don't keep
      // around the fact that the cell was deleted in a newer record, we end up
      // returning the old value if user is asking for more than one version.
      // This List of deletes should not large since we are only keeping rows
      // and columns that match those set on the scanner and which have delete
      // values.  If memory usage becomes an issue, could redo as bloom filter.
      Map<byte [], List<Long>> deletes =
        new TreeMap<byte [], List<Long>>(Bytes.BYTES_COMPARATOR);
      // This code below is very close to the body of the getKeys method.
      MapFile.Reader[] maparray = getReaders();
      for(int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(key, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          if (!readkey.matchesRowCol(key)) {
            continue;
          }
          if (!isDeleted(readkey, readval.get(), true, deletes)) {
            if (ttl == HConstants.FOREVER ||
                    now < readkey.getTimestamp() + ttl) {
              results.add(new Cell(readval.get(), readkey.getTimestamp()));
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("get: " + readkey + ": expired, skipped");
              }
            }
            // Perhaps only one version is wanted.  I could let this
            // test happen later in the for loop test but it would cost
            // the allocation of an ImmutableBytesWritable.
            if (hasEnoughVersions(numVersions, results)) {
              break;
            }
          }
          for (readval = new ImmutableBytesWritable();
              map.next(readkey, readval) &&
              readkey.matchesRowCol(key) &&
              !hasEnoughVersions(numVersions, results);
              readval = new ImmutableBytesWritable()) {
            if (!isDeleted(readkey, readval.get(), true, deletes)) {
              if (ttl == HConstants.FOREVER ||
                    now < readkey.getTimestamp() + ttl) {
                results.add(new Cell(readval.get(), readkey.getTimestamp()));
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("get: " + readkey + ": expired, skipped");
                }
              }
            }
          }
        }
        if (hasEnoughVersions(numVersions, results)) {
          break;
        }
      }
      return results.size() == 0 ?
        null : results.toArray(new Cell[results.size()]);
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Small method to check if we are over the max number of versions
   * or we acheived this family max versions. 
   * The later happens when we have the situation described in HBASE-621.
   * @param numVersions
   * @param results
   * @return 
   */
  private boolean hasEnoughVersions(final int numVersions,
      final List<Cell> results) {
    return (results.size() >= numVersions || results.size() >= family
            .getMaxVersions());
  }

  /**
   * Get <code>versions</code> keys matching the origin key's
   * row/column/timestamp and those of an older vintage
   * Default access so can be accessed out of {@link HRegionServer}.
   * @param origin Where to start searching.
   * @param versions How many versions to return. Pass
   * {@link HConstants.ALL_VERSIONS} to retrieve all. Versions will include
   * size of passed <code>allKeys</code> in its count.
   * @param allKeys List of keys prepopulated by keys we found in memcache.
   * This method returns this passed list with all matching keys found in
   * stores appended.
   * @return The passed <code>allKeys</code> with <code>versions</code> of
   * matching keys found in store files appended.
   * @throws IOException
   */
  List<HStoreKey> getKeys(final HStoreKey origin, final int versions)
  throws IOException {
      
    List<HStoreKey> keys = this.memcache.getKeys(origin, versions);
    if (keys.size() >= versions) {
      return keys;
    }
    
    // This code below is very close to the body of the get method.
    this.lock.readLock().lock();
    long now = System.currentTimeMillis();
    try {
      MapFile.Reader[] maparray = getReaders();
      for(int i = maparray.length - 1; i >= 0; i--) {
        MapFile.Reader map = maparray[i];
        synchronized(map) {
          map.reset();
          
          // do the priming read
          ImmutableBytesWritable readval = new ImmutableBytesWritable();
          HStoreKey readkey = (HStoreKey)map.getClosest(origin, readval);
          if (readkey == null) {
            // map.getClosest returns null if the passed key is > than the
            // last key in the map file.  getClosest is a bit of a misnomer
            // since it returns exact match or the next closest key AFTER not
            // BEFORE.
            continue;
          }
          
          do{
            // if the row matches, we might want this one.
            if (rowMatches(origin, readkey)) {
              // if the cell matches, then we definitely want this key.
              if (cellMatches(origin, readkey)) {
                // store the key if it isn't deleted or superceeded by what's
                // in the memcache
                if (!isDeleted(readkey, readval.get(), false, null) &&
                    !keys.contains(readkey)) {
                  if (ttl == HConstants.FOREVER ||
                        now < readkey.getTimestamp() + ttl) {
                    keys.add(new HStoreKey(readkey));
                  } else {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("getKeys: " + readkey +
                          ": expired, skipped");
                    }
                  }

                  // if we've collected enough versions, then exit the loop.
                  if (keys.size() >= versions) {
                    break;
                  }
                }
              } else {
                // the cell doesn't match, but there might be more with different
                // timestamps, so move to the next key
                continue;
              }
            } else {
              // the row doesn't match, so we've gone too far.
              break;
            }
          } while (map.next(readkey, readval)); // advance to the next key
        }
      }
      
      return keys;
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Find the key that matches <i>row</i> exactly, or the one that immediately
   * preceeds it. WARNING: Only use this method on a table where writes occur 
   * with stricly increasing timestamps. This method assumes this pattern of 
   * writes in order to make it reasonably performant. 
   */
  byte [] getRowKeyAtOrBefore(final byte [] row)
  throws IOException{
    // Map of HStoreKeys that are candidates for holding the row key that
    // most closely matches what we're looking for. We'll have to update it 
    // deletes found all over the place as we go along before finally reading
    // the best key out of it at the end.
    SortedMap<HStoreKey, Long> candidateKeys = new TreeMap<HStoreKey, Long>();
    
    // Obtain read lock
    this.lock.readLock().lock();
    try {
      // Process each store file
      MapFile.Reader[] maparray = getReaders();
      for (int i = maparray.length - 1; i >= 0; i--) {
        // update the candidate keys from the current map file
        rowAtOrBeforeFromMapFile(maparray[i], row, candidateKeys);
      }
      
      // Finally, check the memcache
      this.memcache.getRowKeyAtOrBefore(row, candidateKeys);
      
      // Return the best key from candidateKeys
      return candidateKeys.isEmpty()? null: candidateKeys.lastKey().getRow();
    } finally {
      this.lock.readLock().unlock();
    }
  }
  
  /**
   * Check an individual MapFile for the row at or before a given key 
   * and timestamp
   */
  private void rowAtOrBeforeFromMapFile(MapFile.Reader map, final byte [] row, 
    SortedMap<HStoreKey, Long> candidateKeys)
  throws IOException {
    HStoreKey searchKey = null;
    ImmutableBytesWritable readval = new ImmutableBytesWritable();
    HStoreKey readkey = new HStoreKey();
    HStoreKey strippedKey = null;
    synchronized(map) {
      // don't bother with the rest of this if the file is empty
      map.reset();
      if (!map.next(readkey, readval)) {
        return;
      }

      long now = System.currentTimeMillis();
      // if there aren't any candidate keys yet, we'll do some things slightly
      // different 
      if (candidateKeys.isEmpty()) {
        searchKey = new HStoreKey(row);
        
        // if the row we're looking for is past the end of this mapfile, just
        // save time and add the last key to the candidates.
        HStoreKey finalKey = new HStoreKey(); 
        map.finalKey(finalKey);
        if (Bytes.compareTo(finalKey.getRow(), row) < 0) {
          candidateKeys.put(stripTimestamp(finalKey), 
            new Long(finalKey.getTimestamp()));
          return;
        }
        
        // seek to the exact row, or the one that would be immediately before it
        readkey = (HStoreKey)map.getClosest(searchKey, readval, true);
      
        if (readkey == null) {
          // didn't find anything that would match, so return
          return;
        }
      
        do {
          // if we have an exact match on row, and it's not a delete, save this
          // as a candidate key
          if (Bytes.equals(readkey.getRow(), row)) {
            if (!HLogEdit.isDeleted(readval.get())) {
              if (ttl == HConstants.FOREVER || 
                    now < readkey.getTimestamp() + ttl) {
                candidateKeys.put(stripTimestamp(readkey), 
                  new Long(readkey.getTimestamp()));
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("rowAtOrBeforeFromMapFile:" + readkey +
                    ": expired, skipped");
                }
              }
            }
          } else if (Bytes.compareTo(readkey.getRow(), row) > 0 ) {
            // if the row key we just read is beyond the key we're searching for,
            // then we're done. return.
            return;
          } else {
            // so, the row key doesn't match, but we haven't gone past the row
            // we're seeking yet, so this row is a candidate for closest
            // (assuming that it isn't a delete).
            if (!HLogEdit.isDeleted(readval.get())) {
              if (ttl == HConstants.FOREVER || 
                      now < readkey.getTimestamp() + ttl) {
                candidateKeys.put(stripTimestamp(readkey), 
                  new Long(readkey.getTimestamp()));
              } else {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("rowAtOrBeforeFromMapFile:" + readkey +
                    ": expired, skipped");
                }
              }
            }
          }        
        } while(map.next(readkey, readval));
      
        // arriving here just means that we consumed the whole rest of the map
        // without going "past" the key we're searching for. we can just fall
        // through here.
      } else {
        // if there are already candidate keys, we need to start our search 
        // at the earliest possible key so that we can discover any possible
        // deletes for keys between the start and the search key.
        searchKey = new HStoreKey(candidateKeys.firstKey().getRow());
              
        // if the row we're looking for is past the end of this mapfile, just
        // save time and add the last key to the candidates.
        HStoreKey finalKey = new HStoreKey(); 
        map.finalKey(finalKey);
        if (Bytes.compareTo(finalKey.getRow(), searchKey.getRow()) < 0) {
          strippedKey = stripTimestamp(finalKey);
          
          // if the candidate keys has a cell like this one already,
          // then we might want to update the timestamp we're using on it
          if (candidateKeys.containsKey(strippedKey)) {
            long bestCandidateTs = 
              candidateKeys.get(strippedKey).longValue();
            if (bestCandidateTs < finalKey.getTimestamp()) {
              candidateKeys.put(strippedKey, new Long(finalKey.getTimestamp()));
            } 
          } else {
            // otherwise, this is a new key, so put it up as a candidate
            candidateKeys.put(strippedKey, new Long(finalKey.getTimestamp()));
          }
        }
        return;
      }
      
      // seek to the exact row, or the one that would be immediately before it
      readkey = (HStoreKey)map.getClosest(searchKey, readval, true);
      
      if (readkey == null) {
        // didn't find anything that would match, so return
        return;
      }
      
      do {
        // if we have an exact match on row, and it's not a delete, save this
        // as a candidate key
        if (Bytes.equals(readkey.getRow(), row)) {
          strippedKey = stripTimestamp(readkey);
          if (!HLogEdit.isDeleted(readval.get())) {
            if (ttl == HConstants.FOREVER || 
                    now < readkey.getTimestamp() + ttl) {
              candidateKeys.put(strippedKey,
                new Long(readkey.getTimestamp()));
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("rowAtOrBeforeFromMapFile: " + readkey +
                  ": expired, skipped");
              }
            }
          } else {
            // if the candidate keys contain any that might match by timestamp,
            // then check for a match and remove it if it's too young to 
            // survive the delete 
            if (candidateKeys.containsKey(strippedKey)) {
              long bestCandidateTs =
                candidateKeys.get(strippedKey).longValue();
              if (bestCandidateTs <= readkey.getTimestamp()) {
                candidateKeys.remove(strippedKey);
              } 
            }
          }
        } else if (Bytes.compareTo(readkey.getRow(), row) > 0 ) {
          // if the row key we just read is beyond the key we're searching for,
          // then we're done. return.
          return;
        } else {
          strippedKey = stripTimestamp(readkey);
          
          // so, the row key doesn't match, but we haven't gone past the row
          // we're seeking yet, so this row is a candidate for closest 
          // (assuming that it isn't a delete).
          if (!HLogEdit.isDeleted(readval.get())) {
            if (ttl == HConstants.FOREVER || 
                    now < readkey.getTimestamp() + ttl) {
              candidateKeys.put(strippedKey, readkey.getTimestamp());
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("rowAtOrBeforeFromMapFile: " + readkey +
                  ": expired, skipped");
              }
            }
          } else {
            // if the candidate keys contain any that might match by timestamp,
            // then check for a match and remove it if it's too young to 
            // survive the delete 
            if (candidateKeys.containsKey(strippedKey)) {
              long bestCandidateTs = 
                candidateKeys.get(strippedKey).longValue();
              if (bestCandidateTs <= readkey.getTimestamp()) {
                candidateKeys.remove(strippedKey);
              } 
            }
          }      
        }
      } while(map.next(readkey, readval));
    }
  }
  
  static HStoreKey stripTimestamp(HStoreKey key) {
    return new HStoreKey(key.getRow(), key.getColumn());
  }
    
  /**
   * Test that the <i>target</i> matches the <i>origin</i>. If the 
   * <i>origin</i> has an empty column, then it's assumed to mean any column 
   * matches and only match on row and timestamp. Otherwise, it compares the
   * keys with HStoreKey.matchesRowCol().
   * @param origin The key we're testing against
   * @param target The key we're testing
   */
  private boolean cellMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (Bytes.equals(origin.getColumn(), HConstants.EMPTY_BYTE_ARRAY)){
      // if the row matches, then...
      if (Bytes.equals(target.getRow(), origin.getRow())) {
        // check the timestamp
        return target.getTimestamp() <= origin.getTimestamp();
      }
      return false;
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
    
  /**
   * Test that the <i>target</i> matches the <i>origin</i>. If the <i>origin</i>
   * has an empty column, then it just tests row equivalence. Otherwise, it uses
   * HStoreKey.matchesRowCol().
   * @param origin Key we're testing against
   * @param target Key we're testing
   */
  private boolean rowMatches(HStoreKey origin, HStoreKey target){
    // if the origin's column is empty, then we're matching any column
    if (Bytes.equals(origin.getColumn(), HConstants.EMPTY_BYTE_ARRAY)) {
      // if the row matches, then...
      return Bytes.equals(target.getRow(), origin.getRow());
    }
    // otherwise, we want to match on row and column
    return target.matchesRowCol(origin);
  }
  
  /**
   * Determines if HStore can be split
   * 
   * @return midKey if store can be split, null otherwise
   */
  byte [] checkSplit() {
    if (this.storefiles.size() <= 0) {
      return null;
    }
    if (storeSize < this.desiredMaxFileSize) {
      return null;
    }
    this.lock.readLock().lock();
    try {
      // Not splitable if we find a reference store file present in the store.
      boolean splitable = true;
      long maxSize = 0L;
      Long mapIndex = Long.valueOf(0L);
      // Iterate through all the MapFiles
      synchronized (storefiles) {
        for (Map.Entry<Long, HStoreFile> e: storefiles.entrySet()) {
          HStoreFile curHSF = e.getValue();
          long size = curHSF.length();
          if (size > maxSize) {
            // This is the largest one so far
            maxSize = size;
            mapIndex = e.getKey();
          }
          if (splitable) {
            splitable = !curHSF.isReference();
          }
        }
      }
      if (!splitable) {
        return null;
      }
      MapFile.Reader r = this.readers.get(mapIndex);

      // seek back to the beginning of mapfile
      r.reset();

      // get the first and last keys
      HStoreKey firstKey = new HStoreKey();
      HStoreKey lastKey = new HStoreKey();
      Writable value = new ImmutableBytesWritable();
      r.next(firstKey, value);
      r.finalKey(lastKey);

      // get the midkey
      HStoreKey mk = (HStoreKey)r.midKey();
      if (mk != null) {
        // if the midkey is the same as the first and last keys, then we cannot
        // (ever) split this region. 
        if (Bytes.equals(mk.getRow(), firstKey.getRow()) && 
            Bytes.equals(mk.getRow(), lastKey.getRow())) {
          return null;
        }
        return mk.getRow();
      }
    } catch(IOException e) {
      LOG.warn("Failed getting store size for " + this.storeNameStr, e);
    } finally {
      this.lock.readLock().unlock();
    }
    return null;
  }
  
  /** @return aggregate size of HStore */
  public long getSize() {
    return storeSize;
  }
     
  //////////////////////////////////////////////////////////////////////////////
  // File administration
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Return a scanner for both the memcache and the HStore files
   */
  InternalScanner getScanner(long timestamp, byte [][] targetCols,
      byte [] firstRow, RowFilterInterface filter)
  throws IOException {
    lock.readLock().lock();
    try {
      return new HStoreScanner(this, targetCols, firstRow, timestamp, filter);
    } finally {
      lock.readLock().unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return this.storeNameStr;
  }

  /**
   * @param p Path to check.
   * @return True if the path has format of a HStoreFile reference.
   */
  public static boolean isReference(final Path p) {
    return isReference(p, REF_NAME_PARSER.matcher(p.getName()));
  }
 
  private static boolean isReference(final Path p, final Matcher m) {
    if (m == null || !m.matches()) {
      LOG.warn("Failed match of store file name " + p.toString());
      throw new RuntimeException("Failed match of store file name " +
          p.toString());
    }
    return m.groupCount() > 1 && m.group(2) != null;
  }

  /**
   * @return Current list of store files.
   */
  SortedMap<Long, HStoreFile> getStorefiles() {
    synchronized (this.storefiles) {
      SortedMap<Long, HStoreFile> copy =
        new TreeMap<Long, HStoreFile>(this.storefiles);
      return copy;
    }
  }
}
