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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.LeaseException;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.client.transactional.UnknownTransactionException;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionState.Status;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Progressable;

/**
 * Regionserver which provides transactional support for atomic transactions.
 * This is achieved with optimistic concurrency control (see
 * http://www.seas.upenn.edu/~zives/cis650/papers/opt-cc.pdf). We keep track
 * read and write sets for each transaction, and hold off on processing the
 * writes. To decide to commit a transaction we check its read sets with all
 * transactions that have committed while it was running for overlaps.
 * <p>
 * Because transactions can span multiple regions, all regions must agree to
 * commit a transactions. The client side of this commit protocol is encoded in
 * org.apache.hadoop.hbase.client.transactional.TransactionManger
 * <p>
 * In the event of an failure of the client mid-commit, (after we voted yes), we
 * will have to consult the transaction log to determine the final decision of
 * the transaction. This is not yet implemented.
 */
class TransactionalRegion extends HRegion {

  private static final String LEASE_TIME = "hbase.transaction.leaseTime";
  private static final int DEFAULT_LEASE_TIME = 60 * 1000;
  private static final int LEASE_CHECK_FREQUENCY = 1000;
  
  private static final String OLD_TRANSACTION_FLUSH = "hbase.transaction.flush";
  private static final int DEFAULT_OLD_TRANSACTION_FLUSH = 100; // Do a flush if we have this many old transactions..
  

  private static final Log LOG = LogFactory.getLog(TransactionalRegion.class);

  // Collection of active transactions (PENDING) keyed by id.
  private Map<String, TransactionState> transactionsById = new HashMap<String, TransactionState>();

  // Map of recent transactions that are COMMIT_PENDING or COMMITED keyed by
  // their sequence number
  private SortedMap<Integer, TransactionState> commitedTransactionsBySequenceNumber = Collections
      .synchronizedSortedMap(new TreeMap<Integer, TransactionState>());

  // Collection of transactions that are COMMIT_PENDING
  private Set<TransactionState> commitPendingTransactions = Collections
      .synchronizedSet(new HashSet<TransactionState>());

  private final Leases transactionLeases;
  private AtomicInteger nextSequenceId = new AtomicInteger(0);
  private Object commitCheckLock = new Object();
  private TransactionalHLogManager logManager;
  private final int oldTransactionFlushTrigger;

  public TransactionalRegion(final Path basedir, final HLog log,
      final FileSystem fs, final HBaseConfiguration conf,
      final HRegionInfo regionInfo, final FlushRequester flushListener) {
    super(basedir, log, fs, conf, regionInfo, flushListener);
    transactionLeases = new Leases(conf.getInt(LEASE_TIME, DEFAULT_LEASE_TIME),
        LEASE_CHECK_FREQUENCY);
    logManager = new TransactionalHLogManager(this);
    oldTransactionFlushTrigger = conf.getInt(OLD_TRANSACTION_FLUSH, DEFAULT_OLD_TRANSACTION_FLUSH);
  }

  @Override
  protected void doReconstructionLog(final Path oldLogFile,
      final long minSeqId, final long maxSeqId, final Progressable reporter)
      throws UnsupportedEncodingException, IOException {
    super.doReconstructionLog(oldLogFile, minSeqId, maxSeqId, reporter);

    Map<Long, List<BatchUpdate>> commitedTransactionsById = logManager
        .getCommitsFromLog(oldLogFile, minSeqId, reporter);

    if (commitedTransactionsById != null && commitedTransactionsById.size() > 0) {
      LOG.debug("found " + commitedTransactionsById.size()
          + " COMMITED transactions");

      for (Entry<Long, List<BatchUpdate>> entry : commitedTransactionsById
          .entrySet()) {
        LOG.debug("Writing " + entry.getValue().size()
            + " updates for transaction " + entry.getKey());
        for (BatchUpdate b : entry.getValue()) {
          super.batchUpdate(b, true); // These are walled so they live forever
        }
      }

      // LOG.debug("Flushing cache"); // We must trigger a cache flush,
      // otherwise
      // we will would ignore the log on subsequent failure
      // if (!super.flushcache()) {
      // LOG.warn("Did not flush cache");
      // }
    }
  }

  /**
   * We need to make sure that we don't complete a cache flush between running
   * transactions. If we did, then we would not find all log messages needed to
   * restore the transaction, as some of them would be before the last
   * "complete" flush id.
   */
  @Override
  protected long getCompleteCacheFlushSequenceId(final long currentSequenceId) {
    long minPendingStartSequenceId = currentSequenceId;
    for (TransactionState transactionState : transactionsById.values()) {
      minPendingStartSequenceId = Math.min(minPendingStartSequenceId,
          transactionState.getHLogStartSequenceId());
    }
    return minPendingStartSequenceId;
  }

  public void beginTransaction(final long transactionId) throws IOException {
    String key = String.valueOf(transactionId);
    if (transactionsById.get(key) != null) {
      TransactionState alias = getTransactionState(transactionId);
      if (alias != null) {
        alias.setStatus(Status.ABORTED);
        retireTransaction(alias);
      }
      throw new IOException("Already exiting transaction id: " + key);
    }

    TransactionState state = new TransactionState(transactionId, super.getLog()
        .getSequenceNumber());

    // Order is important here
    for (TransactionState commitPending : commitPendingTransactions) {
      state.addTransactionToCheck(commitPending);
    }
    state.setStartSequenceNumber(nextSequenceId.get());

    transactionsById.put(String.valueOf(key), state);
    try {
      transactionLeases.createLease(key, new TransactionLeaseListener(key));
    } catch (LeaseStillHeldException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Begining transaction " + key + " in region "
        + super.getRegionInfo().getRegionNameAsString());
    logManager.writeStartToLog(transactionId);
    
    maybeTriggerOldTransactionFlush();
  }

  /**
   * Fetch a single data item.
   * 
   * @param transactionId
   * @param row
   * @param column
   * @return column value
   * @throws IOException
   */
  public Cell get(final long transactionId, final byte[] row,
      final byte[] column) throws IOException {
    Cell[] results = get(transactionId, row, column, 1);
    return (results == null || results.length == 0) ? null : results[0];
  }

  /**
   * Fetch multiple versions of a single data item
   * 
   * @param transactionId
   * @param row
   * @param column
   * @param numVersions
   * @return array of values one element per version
   * @throws IOException
   */
  public Cell[] get(final long transactionId, final byte[] row,
      final byte[] column, final int numVersions) throws IOException {
    return get(transactionId, row, column, Long.MAX_VALUE, numVersions);
  }

  /**
   * Fetch multiple versions of a single data item, with timestamp.
   * 
   * @param transactionId
   * @param row
   * @param column
   * @param timestamp
   * @param numVersions
   * @return array of values one element per version that matches the timestamp
   * @throws IOException
   */
  public Cell[] get(final long transactionId, final byte[] row,
      final byte[] column, final long timestamp, final int numVersions)
      throws IOException {
    TransactionState state = getTransactionState(transactionId);

    state.addRead(row);

    Cell[] localCells = state.localGet(row, column, timestamp);

    if (localCells != null && localCells.length > 0) {
      LOG
          .trace("Transactional get of something we've written in the same transaction "
              + transactionId);
      LOG.trace("row: " + Bytes.toString(row));
      LOG.trace("col: " + Bytes.toString(column));
      LOG.trace("numVersions: " + numVersions);
      for (Cell cell : localCells) {
        LOG.trace("cell: " + Bytes.toString(cell.getValue()));
      }

      if (numVersions > 1) {
        Cell[] globalCells = get(row, column, timestamp, numVersions - 1);
        Cell[] result = new Cell[globalCells.length + localCells.length];
        System.arraycopy(localCells, 0, result, 0, localCells.length);
        System.arraycopy(globalCells, 0, result, localCells.length,
            globalCells.length);
        return result;
      }
      return localCells;
    }

    return get(row, column, timestamp, numVersions);
  }

  /**
   * Fetch all the columns for the indicated row at a specified timestamp.
   * Returns a TreeMap that maps column names to values.
   * 
   * @param transactionId
   * @param row
   * @param columns Array of columns you'd like to retrieve. When null, get all.
   * @param ts
   * @return Map<columnName, Cell> values
   * @throws IOException
   */
  public Map<byte[], Cell> getFull(final long transactionId, final byte[] row,
      final Set<byte[]> columns, final long ts) throws IOException {
    TransactionState state = getTransactionState(transactionId);

    state.addRead(row);

    Map<byte[], Cell> localCells = state.localGetFull(row, columns, ts);

    if (localCells != null && localCells.size() > 0) {
      LOG
          .trace("Transactional get of something we've written in the same transaction "
              + transactionId);
      LOG.trace("row: " + Bytes.toString(row));
      for (Entry<byte[], Cell> entry : localCells.entrySet()) {
        LOG.trace("col: " + Bytes.toString(entry.getKey()));
        LOG.trace("cell: " + Bytes.toString(entry.getValue().getValue()));
      }

      Map<byte[], Cell> internalResults = getFull(row, columns, ts, null);
      internalResults.putAll(localCells);
      return internalResults;
    }

    return getFull(row, columns, ts, null);
  }

  /**
   * Return an iterator that scans over the HRegion, returning the indicated
   * columns for only the rows that match the data filter. This Iterator must be
   * closed by the caller.
   * 
   * @param transactionId
   * @param cols columns to scan. If column name is a column family, all columns
   * of the specified column family are returned. Its also possible to pass a
   * regex in the column qualifier. A column qualifier is judged to be a regex
   * if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param firstRow row which is the starting point of the scan
   * @param timestamp only return rows whose timestamp is <= this value
   * @param filter row filter
   * @return InternalScanner
   * @throws IOException
   */
  public InternalScanner getScanner(final long transactionId,
      final byte[][] cols, final byte[] firstRow, final long timestamp,
      final RowFilterInterface filter) throws IOException {
    return new ScannerWrapper(transactionId, super.getScanner(cols, firstRow,
        timestamp, filter));
  }

  /**
   * Add a write to the transaction. Does not get applied until commit process.
   * 
   * @param b
   * @throws IOException
   */
  public void batchUpdate(final long transactionId, final BatchUpdate b)
      throws IOException {
    TransactionState state = getTransactionState(transactionId);
    state.addWrite(b);
    logManager.writeUpdateToLog(transactionId, b);
  }

  /**
   * Add a delete to the transaction. Does not get applied until commit process.
   * FIXME, not sure about this approach
   * 
   * @param b
   * @throws IOException
   */
  public void deleteAll(final long transactionId, final byte[] row,
      final long timestamp) throws IOException {
    TransactionState state = getTransactionState(transactionId);
    long now = System.currentTimeMillis();

    for (HStore store : super.stores.values()) {
      List<HStoreKey> keys = store.getKeys(new HStoreKey(row, timestamp),
          ALL_VERSIONS, now);
      BatchUpdate deleteUpdate = new BatchUpdate(row, timestamp);

      for (HStoreKey key : keys) {
        deleteUpdate.delete(key.getColumn());
      }
      
      state.addWrite(deleteUpdate);
      logManager.writeUpdateToLog(transactionId, deleteUpdate);

    }

  }

  public boolean commitRequest(final long transactionId) throws IOException {
    synchronized (commitCheckLock) {
      TransactionState state = getTransactionState(transactionId);
      if (state == null) {
        return false;
      }

      if (hasConflict(state)) {
        state.setStatus(Status.ABORTED);
        retireTransaction(state);
        return false;
      }

      // No conflicts, we can commit.
      LOG.trace("No conflicts for transaction " + transactionId
          + " found in region " + super.getRegionInfo().getRegionNameAsString()
          + ". Voting for commit");
      state.setStatus(Status.COMMIT_PENDING);

      // If there are writes we must keep record off the transaction
      if (state.getWriteSet().size() > 0) {
        // Order is important
        commitPendingTransactions.add(state);
        state.setSequenceNumber(nextSequenceId.getAndIncrement());
        commitedTransactionsBySequenceNumber.put(state.getSequenceNumber(),
            state);
      }

      return true;
    }
  }

  private boolean hasConflict(final TransactionState state) {
    // Check transactions that were committed while we were running
    for (int i = state.getStartSequenceNumber(); i < nextSequenceId.get(); i++) {
      TransactionState other = commitedTransactionsBySequenceNumber.get(i);
      if (other == null) {
        continue;
      }
      state.addTransactionToCheck(other);
    }

    return state.hasConflict();
  }

  /**
   * Commit the transaction.
   * 
   * @param transactionId
   * @return
   * @throws IOException
   */
  public void commit(final long transactionId) throws IOException {
    TransactionState state;
    try {
      state = getTransactionState(transactionId);
    } catch (UnknownTransactionException e) {
      LOG.fatal("Asked to commit unknown transaction: " + transactionId
          + " in region " + super.getRegionInfo().getRegionNameAsString());
      // FIXME Write to the transaction log that this transaction was corrupted
      throw e;
    }

    if (!state.getStatus().equals(Status.COMMIT_PENDING)) {
      LOG.fatal("Asked to commit a non pending transaction");
      // FIXME Write to the transaction log that this transaction was corrupted
      throw new IOException("commit failure");
    }

    commit(state);
  }

  /**
   * Commit the transaction.
   * 
   * @param transactionId
   * @return
   * @throws IOException
   */
  public void abort(final long transactionId) throws IOException {
    TransactionState state;
    try {
      state = getTransactionState(transactionId);
    } catch (UnknownTransactionException e) {
      LOG.error("Asked to abort unknown transaction: " + transactionId);
      return;
    }

    state.setStatus(Status.ABORTED);

    if (state.getWriteSet().size() > 0) {
      logManager.writeAbortToLog(state.getTransactionId());
    }

    // Following removes needed if we have voted
    if (state.getSequenceNumber() != null) {
      commitedTransactionsBySequenceNumber.remove(state.getSequenceNumber());
    }
    commitPendingTransactions.remove(state);

    retireTransaction(state);
  }

  private void commit(final TransactionState state) throws IOException {

    LOG.debug("Commiting transaction: " + state.toString() + " to "
        + super.getRegionInfo().getRegionNameAsString());

    if (state.getWriteSet().size() > 0) {
      logManager.writeCommitToLog(state.getTransactionId());
    }

    for (BatchUpdate update : state.getWriteSet()) {
      super.batchUpdate(update, false); // Don't need to WAL these
      // FIME, maybe should be walled so we don't need to look so far back.
    }

    state.setStatus(Status.COMMITED);
    if (state.getWriteSet().size() > 0
        && !commitPendingTransactions.remove(state)) {
      LOG
          .fatal("Commiting a non-query transaction that is not in commitPendingTransactions");
      throw new IOException("commit failure"); // FIXME, how to handle?
    }
    retireTransaction(state);
  }

  // Cancel leases, and removed from lease lookup. This transaction may still
  // live in commitedTransactionsBySequenceNumber and commitPendingTransactions
  private void retireTransaction(final TransactionState state) {
    String key = String.valueOf(state.getTransactionId());
    try {
      transactionLeases.cancelLease(key);
    } catch (LeaseException e) {
      // Ignore
    }

    transactionsById.remove(key);
  }

  private TransactionState getTransactionState(final long transactionId)
      throws UnknownTransactionException {
    String key = String.valueOf(transactionId);
    TransactionState state = null;

    state = transactionsById.get(key);

    if (state == null) {
      LOG.trace("Unknown transaction: " + key);
      throw new UnknownTransactionException(key);
    }

    try {
      transactionLeases.renewLease(key);
    } catch (LeaseException e) {
      throw new RuntimeException(e);
    }

    return state;
  }

  private void maybeTriggerOldTransactionFlush() {
      if (commitedTransactionsBySequenceNumber.size() > oldTransactionFlushTrigger) {
        removeUnNeededCommitedTransactions();
      }
  }
  
  /**
   * Cleanup references to committed transactions that are no longer needed.
   * 
   */
  synchronized void removeUnNeededCommitedTransactions() {
    Integer minStartSeqNumber = getMinStartSequenceNumber();
    if (minStartSeqNumber == null) {
      minStartSeqNumber = Integer.MAX_VALUE; // Remove all
    }

    int numRemoved = 0;
    // Copy list to avoid conc update exception
    for (Entry<Integer, TransactionState> entry : new LinkedList<Entry<Integer, TransactionState>>(
        commitedTransactionsBySequenceNumber.entrySet())) {
      if (entry.getKey() >= minStartSeqNumber) {
        break;
      }
      numRemoved = numRemoved
          + (commitedTransactionsBySequenceNumber.remove(entry.getKey()) == null ? 0
              : 1);
      numRemoved++;
    }

    if (numRemoved > 0) {
      LOG.debug("Removed " + numRemoved
          + " commited transactions with sequence lower than "
          + minStartSeqNumber + ". Still have "
          + commitedTransactionsBySequenceNumber.size() + " left");
    } else if (commitedTransactionsBySequenceNumber.size() > 0) {
      LOG.debug("Could not remove any transactions, and still have "
          + commitedTransactionsBySequenceNumber.size() + " left");
    }
  }

  private Integer getMinStartSequenceNumber() {
    Integer min = null;
    for (TransactionState transactionState : transactionsById.values()) {
      if (min == null || transactionState.getStartSequenceNumber() < min) {
        min = transactionState.getStartSequenceNumber();
      }
    }
    return min;
  }

  // TODO, resolve from the global transaction log
  @SuppressWarnings("unused")
  private void resolveTransactionFromLog(final long transactionId) {
    throw new RuntimeException("Globaql transaction log is not Implemented");
  }

  private class TransactionLeaseListener implements LeaseListener {
    private final String transactionName;

    TransactionLeaseListener(final String n) {
      this.transactionName = n;
    }

    /** {@inheritDoc} */
    public void leaseExpired() {
      LOG.info("Transaction " + this.transactionName + " lease expired");
      TransactionState s = null;
      synchronized (transactionsById) {
        s = transactionsById.remove(transactionName);
      }
      if (s == null) {
        LOG.warn("Unknown transaction expired " + this.transactionName);
        return;
      }

      switch (s.getStatus()) {
      case PENDING:
        s.setStatus(Status.ABORTED); // Other transactions may have a ref
        break;
      case COMMIT_PENDING:
        LOG.info("Transaction " + s.getTransactionId()
            + " expired in COMMIT_PENDING state");
        LOG.info("Checking transaction status in transaction log");
        resolveTransactionFromLog(s.getTransactionId());
        break;
      default:
        LOG.warn("Unexpected status on expired lease");
      }
    }
  }

  /** Wrapper which keeps track of rows returned by scanner. */
  private class ScannerWrapper implements InternalScanner {
    private long transactionId;
    private InternalScanner scanner;

    public ScannerWrapper(final long transactionId,
        final InternalScanner scanner) {
      this.transactionId = transactionId;
      this.scanner = scanner;
    }

    public void close() throws IOException {
      scanner.close();
    }

    public boolean isMultipleMatchScanner() {
      return scanner.isMultipleMatchScanner();
    }

    public boolean isWildcardScanner() {
      return scanner.isWildcardScanner();
    }

    public boolean next(final HStoreKey key,
        final SortedMap<byte[], Cell> results) throws IOException {
      boolean result = scanner.next(key, results);
      TransactionState state = getTransactionState(transactionId);
      state.setHasScan(true);
      // FIXME, not using row, just claiming read over the whole region. We are
      // being very conservative on scans to avoid phantom reads.
      state.addRead(key.getRow());

      if (result) {
        Map<byte[], Cell> localWrites = state.localGetFull(key.getRow(), null,
            Integer.MAX_VALUE);
        if (localWrites != null) {
          LOG
              .info("Scanning over row that has been writen to "
                  + transactionId);
          for (Entry<byte[], Cell> entry : localWrites.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
          }
        }
      }

      return result;
    }
  }
}
