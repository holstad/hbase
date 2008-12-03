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
package org.apache.hadoop.hbase.regionserver.tableindexed;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTable;
import org.apache.hadoop.hbase.io.BatchOperation;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HLog;
import org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion;
import org.apache.hadoop.hbase.util.Bytes;

class IndexedRegion extends TransactionalRegion {

  private static final Log LOG = LogFactory.getLog(IndexedRegion.class);

  private final HBaseConfiguration conf;
  private Map<IndexSpecification, HTable> indexSpecToTable = new HashMap<IndexSpecification, HTable>();

  public IndexedRegion(final Path basedir, final HLog log, final FileSystem fs,
      final HBaseConfiguration conf, final HRegionInfo regionInfo,
      final FlushRequester flushListener) {
    super(basedir, log, fs, conf, regionInfo, flushListener);
    this.conf = conf;
  }

  private synchronized HTable getIndexTable(IndexSpecification index)
      throws IOException {
    HTable indexTable = indexSpecToTable.get(index);
    if (indexTable == null) {
      indexTable = new HTable(conf, index.getIndexedTableName(super
          .getRegionInfo().getTableDesc().getName()));
      indexSpecToTable.put(index, indexTable);
    }
    return indexTable;
  }

  private Collection<IndexSpecification> getIndexes() {
    return super.getRegionInfo().getTableDesc().getIndexes();
  }

  /**
   * @param batchUpdate
   * @param lockid
   * @param writeToWAL if true, then we write this update to the log
   * @throws IOException
   */
  @Override
  public void batchUpdate(BatchUpdate batchUpdate, Integer lockid, boolean writeToWAL)
      throws IOException {
    updateIndexes(batchUpdate); // Do this first because will want to see the old row
    super.batchUpdate(batchUpdate, lockid, writeToWAL);
  }

  private void updateIndexes(BatchUpdate batchUpdate) throws IOException {
    List<IndexSpecification> indexesToUpdate = new LinkedList<IndexSpecification>();

    // Find the indexes we need to update
    for (IndexSpecification index : getIndexes()) {
      if (possiblyAppliesToIndex(index, batchUpdate)) {
        indexesToUpdate.add(index);
      }
    }

    if (indexesToUpdate.size() == 0) {
      return;
    }

    Set<byte[]> neededColumns = getColumnsForIndexes(indexesToUpdate);

    SortedMap<byte[], byte[]> newColumnValues = getColumnsFromBatchUpdate(batchUpdate);
    Map<byte[], Cell> oldColumnCells = super.getFull(batchUpdate.getRow(),
        neededColumns, HConstants.LATEST_TIMESTAMP, 1, null);
    
    // Handle delete batch updates. Go back and get the next older values
    for (BatchOperation op : batchUpdate) {
      if (!op.isPut()) {
        Cell current = oldColumnCells.get(op.getColumn());
        if (current != null) {
          Cell [] older = super.get(batchUpdate.getRow(), op.getColumn(), current.getTimestamp(), 1);
          if (older != null && older.length > 0) {
            newColumnValues.put(op.getColumn(), older[0].getValue());
          }
        }
      }
    }
    
    // Add the old values to the new if they are not there
    for (Entry<byte[], Cell> oldEntry : oldColumnCells.entrySet()) {
      if (!newColumnValues.containsKey(oldEntry.getKey())) {
        newColumnValues.put(oldEntry.getKey(), oldEntry.getValue().getValue());
      }
    }
    
  
    
    Iterator<IndexSpecification> indexIterator = indexesToUpdate.iterator();
    while (indexIterator.hasNext()) {
      IndexSpecification indexSpec = indexIterator.next();
      if (!doesApplyToIndex(indexSpec, newColumnValues)) {
        indexIterator.remove();
      }
    }

    SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldColumnCells);
    
    for (IndexSpecification indexSpec : indexesToUpdate) {
      removeOldIndexEntry(indexSpec, batchUpdate.getRow(), oldColumnValues);
      updateIndex(indexSpec, batchUpdate.getRow(), newColumnValues);
    }
  }

  /** Return the columns needed for the update. */
  private Set<byte[]> getColumnsForIndexes(Collection<IndexSpecification> indexes) {
    Set<byte[]> neededColumns = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (IndexSpecification indexSpec : indexes) {
      for (byte[] col : indexSpec.getAllColumns()) {
        neededColumns.add(col);
      }
    }
    return neededColumns;
  }

  private void removeOldIndexEntry(IndexSpecification indexSpec, byte[] row,
      SortedMap<byte[], byte[]> oldColumnValues) throws IOException {
    for (byte[] indexedCol : indexSpec.getIndexedColumns()) {
      if (!oldColumnValues.containsKey(indexedCol)) {
        LOG.debug("Index [" + indexSpec.getIndexId()
            + "] not trying to remove old entry for row ["
            + Bytes.toString(row) + "] because col ["
            + Bytes.toString(indexedCol) + "] is missing");
        return;
      }
    }

    byte[] oldIndexRow = indexSpec.getKeyGenerator().createIndexKey(row,
        oldColumnValues);
    LOG.debug("Index [" + indexSpec.getIndexId() + "] removing old entry ["
        + Bytes.toString(oldIndexRow) + "]");
    getIndexTable(indexSpec).deleteAll(oldIndexRow);
  }
  
  private SortedMap<byte[], byte[]> getColumnsFromBatchUpdate(BatchUpdate b) {
    SortedMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
        Bytes.BYTES_COMPARATOR);
    for (BatchOperation op : b) {
      if (op.isPut()) {
        columnValues.put(op.getColumn(), op.getValue());
      }
    }
    return columnValues;
  }

  /** Ask if this update *could* apply to the index. It may actually apply if some of the columns needed are missing.
   * 
   * @param indexSpec
   * @param b
   * @return true if possibly apply.
   */
  private boolean possiblyAppliesToIndex(IndexSpecification indexSpec, BatchUpdate b) {
    for (BatchOperation op : b) {
      if (indexSpec.containsColumn(op.getColumn())) {
        return true;
      }
    }
    return false;
  }
  
  /** Ask if this update does apply to the index. 
   * 
   * @param indexSpec
   * @param b
   * @return true if possibly apply.
   */
  private boolean doesApplyToIndex(IndexSpecification indexSpec,  SortedMap<byte[], byte[]> columnValues) {
    
    for (byte [] neededCol : indexSpec.getIndexedColumns()) {
      if (! columnValues.containsKey(neededCol))  {
        LOG.debug("Index [" + indexSpec.getIndexId() + "] can't be updated because ["
            + Bytes.toString(neededCol) + "] is missing");
        return false;
      }
    }
    return true;
  }

  private void updateIndex(IndexSpecification indexSpec, byte[] row,
      SortedMap<byte[], byte[]> columnValues) throws IOException {
    BatchUpdate indexUpdate = createIndexUpdate(indexSpec, row, columnValues);
    getIndexTable(indexSpec).commit(indexUpdate);
    LOG.debug("Index [" + indexSpec.getIndexId() + "] adding new entry ["
        + Bytes.toString(indexUpdate.getRow()) + "] for row ["
        + Bytes.toString(row) + "]");

  }

  private BatchUpdate createIndexUpdate(IndexSpecification indexSpec,
      byte[] row, SortedMap<byte[], byte[]> columnValues) {
    byte[] indexRow = indexSpec.getKeyGenerator().createIndexKey(row,
        columnValues);
    BatchUpdate update = new BatchUpdate(indexRow);

    update.put(IndexedTable.INDEX_BASE_ROW_COLUMN, row);

    for (byte[] col : indexSpec.getIndexedColumns()) {
      byte[] val = columnValues.get(col);
      if (val == null) {
        throw new RuntimeException("Unexpected missing column value. ["+Bytes.toString(col)+"]");
      }
      update.put(col, val);
    }
    
    for (byte [] col : indexSpec.getAdditionalColumns()) {
      byte[] val = columnValues.get(col);
      if (val != null) {
        update.put(col, val);
      }
    }

    return update;
  }

  @Override
  public void deleteAll(final byte[] row, final long ts, final Integer lockid)
      throws IOException {

    if (getIndexes().size() != 0) {

      // Need all columns
      Set<byte[]> neededColumns = getColumnsForIndexes(getIndexes());

      Map<byte[], Cell> oldColumnCells = super.getFull(row,
          neededColumns, HConstants.LATEST_TIMESTAMP, 1, null);
      SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(oldColumnCells);
      
      
      for (IndexSpecification indexSpec : getIndexes()) {
        removeOldIndexEntry(indexSpec, row, oldColumnValues);
      }

      // Handle if there is still a version visible.
      if (ts != HConstants.LATEST_TIMESTAMP) {
        Map<byte[], Cell> currentColumnCells = super.getFull(row,
            neededColumns, ts, 1, null);
        SortedMap<byte[], byte[]> currentColumnValues = convertToValueMap(currentColumnCells);
        
        for (IndexSpecification indexSpec : getIndexes()) {
          if (doesApplyToIndex(indexSpec, currentColumnValues)) {
            updateIndex(indexSpec, row, currentColumnValues);
          }
        }
      }
    }
    super.deleteAll(row, ts, lockid);
  }

  private SortedMap<byte[], byte[]> convertToValueMap(
      Map<byte[], Cell> cellMap) {
    SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    for(Entry<byte[], Cell> entry : cellMap.entrySet()) {
      currentColumnValues.put(entry.getKey(), entry.getValue().getValue());
    }
    return currentColumnValues;
  }

  @Override
  public void deleteAll(final byte[] row, byte[] column, final long ts,
      final Integer lockid) throws IOException {
    List<IndexSpecification> indexesToUpdate = new LinkedList<IndexSpecification>();
    
    for(IndexSpecification indexSpec : getIndexes()) {
      if (indexSpec.containsColumn(column)) {
        indexesToUpdate.add(indexSpec);
      }
    }
    
    Set<byte[]> neededColumns = getColumnsForIndexes(indexesToUpdate);
    Map<byte[], Cell> oldColumnCells = super.getFull(row,
        neededColumns, HConstants.LATEST_TIMESTAMP, 1, null);
    SortedMap<byte [], byte[]> oldColumnValues = convertToValueMap(oldColumnCells);
    
    for (IndexSpecification indexSpec : indexesToUpdate) {
      removeOldIndexEntry(indexSpec, row, oldColumnValues);
    }    
    
    // Handle if there is still a version visible.
    if (ts != HConstants.LATEST_TIMESTAMP) {
      Map<byte[], Cell> currentColumnCells = super.getFull(row,
          neededColumns, ts, 1, null);
      SortedMap<byte[], byte[]> currentColumnValues = convertToValueMap(currentColumnCells);
      
      for (IndexSpecification indexSpec : getIndexes()) {
        if (doesApplyToIndex(indexSpec, currentColumnValues)) {
          updateIndex(indexSpec, row, currentColumnValues);
        }
      }
    }
    
    super.deleteAll(row, column, ts, lockid);
  }

}
