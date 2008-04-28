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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.StopRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Used to communicate with a single HBase table
 */
public class HTable implements HConstants {
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  protected final HConnection connection;
  protected final Text tableName;
  protected final long pause;
  protected final int numRetries;
  protected Random rand;
  protected AtomicReference<BatchUpdate> batch;

  protected volatile boolean tableDoesNotExist;
  
  // For row mutation operations
  
  /**
   * Creates an object to access a HBase table
   * 
   * @param conf configuration object
   * @param tableName name of the table
   * @throws IOException
   */
  public HTable(HBaseConfiguration conf, Text tableName) throws IOException {
    this.connection = HConnectionManager.getConnection(conf);
    this.tableName = tableName;
    this.pause = conf.getLong("hbase.client.pause", 10 * 1000);
    this.numRetries = conf.getInt("hbase.client.retries.number", 5);
    this.rand = new Random();
    this.batch = new AtomicReference<BatchUpdate>();
    this.connection.locateRegion(tableName, EMPTY_START_ROW);
  }

  /**
   * Find region location hosting passed row using cached info
   * @param row Row to find.
   * @return Location of row.
   * @throws IOException
   */
  public HRegionLocation getRegionLocation(Text row) throws IOException {
    return this.connection.locateRegion(this.tableName, row);
  }

  /**
   * Find region location hosting passed row
   * @param row Row to find.
   * @param reload If true do not use cache, otherwise bypass.
   * @return Location of row.
   */
  HRegionLocation getRegionLocation(Text row, boolean reload) throws IOException {
    return reload?
      this.connection.relocateRegion(this.tableName, row):
      this.connection.locateRegion(tableName, row);
  }


  /** @return the connection */
  public HConnection getConnection() {
    return connection;
  }
  
  /**
   * Verifies that no update is in progress
   */
  public synchronized void checkUpdateInProgress() {
    updateInProgress(false);
  }
  
  /*
   * Checks to see if an update is in progress
   * 
   * @param updateMustBeInProgress
   *    If true, an update must be in progress. An IllegalStateException will be
   *    thrown if not.
   *    
   *    If false, an update must not be in progress. An IllegalStateException
   *    will be thrown if an update is in progress.
   */
  private void updateInProgress(boolean updateMustBeInProgress) {
    if (updateMustBeInProgress) {
      if (batch.get() == null) {
        throw new IllegalStateException("no update in progress");
      }
    } else {
      if (batch.get() != null) {
        throw new IllegalStateException("update in progress");
      }
    }
  }
  

  /** @return the table name */
  public Text getTableName() {
    return this.tableName;
  }

  /**
   * @return table metadata 
   * @throws IOException
   */
  public HTableDescriptor getMetadata() throws IOException {
    HTableDescriptor [] metas = this.connection.listTables();
    HTableDescriptor result = null;
    for (int i = 0; i < metas.length; i++) {
      if (metas[i].getName().equals(this.tableName)) {
        result = metas[i];
        break;
      }
    }
    return result;
  }

  /**
   * Gets the starting row key for every region in the currently open table
   * @return Array of region starting row keys
   * @throws IOException
   */
  @SuppressWarnings("null")
  public Text[] getStartKeys() throws IOException {
    List<Text> keyList = new ArrayList<Text>();

    long scannerId = -1L;

    Text startRow = new Text(tableName.toString() + ",,999999999999999");
    HRegionLocation metaLocation = null;
    HRegionInterface server;
    
    // scan over the each meta region
    do {
      try{
        // turn the start row into a location
        metaLocation = 
          connection.locateRegion(META_TABLE_NAME, startRow);

        // connect to the server hosting the .META. region
        server = 
          connection.getHRegionConnection(metaLocation.getServerAddress());

        // open a scanner over the meta region
        scannerId = server.openScanner(
          metaLocation.getRegionInfo().getRegionName(),
          new Text[]{COL_REGIONINFO}, tableName, LATEST_TIMESTAMP,
          null);
        
        // iterate through the scanner, accumulating unique region names
        while (true) {
          RowResult values = server.next(scannerId);
          if (values == null || values.size() == 0) {
            break;
          }
          
          HRegionInfo info = new HRegionInfo();
          info = (HRegionInfo) Writables.getWritable(
            values.get(COL_REGIONINFO).getValue(), info);
          
          if (!info.getTableDesc().getName().equals(this.tableName)) {
            break;
          }
          
          if (info.isOffline() || info.isSplit()) {
            continue;
          }
          keyList.add(info.getStartKey());
        }
        
        // close that remote scanner
        server.close(scannerId);
          
        // advance the startRow to the end key of the current region
        startRow = metaLocation.getRegionInfo().getEndKey();          
      } catch (IOException e) {
        // need retry logic?
        throw e;
      }
    } while (startRow.compareTo(EMPTY_START_ROW) != 0);

    Text[] arr = new Text[keyList.size()];
    for (int i = 0; i < keyList.size(); i++ ){
      arr[i] = keyList.get(i);
    }
    
    return arr;
  }
  
  /** 
   * Get a single value for the specified row and column
   *
   * @param row row key
   * @param column column name
   * @return value for specified row/column
   * @throws IOException
   */
  public Cell get(final Text row, final Text column) throws IOException {
    return getRegionServerWithRetries(new ServerCallable<Cell>(row){
      public Cell call() throws IOException {
        return server.get(location.getRegionInfo().getRegionName(), row, column);
      }
    });
  }
    
  /** 
   * Get the specified number of versions of the specified row and column
   * 
   * @param row         - row key
   * @param column      - column name
   * @param numVersions - number of versions to retrieve
   * @return            - array byte values
   * @throws IOException
   */
  public Cell[] get(final Text row, final Text column, final int numVersions) 
  throws IOException {
    Cell[] values = null;

    values = getRegionServerWithRetries(new ServerCallable<Cell[]>(row) {
      public Cell[] call() throws IOException {
        return server.get(location.getRegionInfo().getRegionName(), row, 
          column, numVersions);
      }
    });

    if (values != null) {
      ArrayList<Cell> cellValues = new ArrayList<Cell>();
      for (int i = 0 ; i < values.length; i++) {
        cellValues.add(values[i]);
      }
      return cellValues.toArray(new Cell[values.length]);
    }
    return null;
  }

  /** 
   * Get the specified number of versions of the specified row and column with
   * the specified timestamp.
   *
   * @param row         - row key
   * @param column      - column name
   * @param timestamp   - timestamp
   * @param numVersions - number of versions to retrieve
   * @return            - array of values that match the above criteria
   * @throws IOException
   */
  public Cell[] get(final Text row, final Text column, final long timestamp, 
    final int numVersions)
  throws IOException {
    Cell[] values = null;

    values = getRegionServerWithRetries(new ServerCallable<Cell[]>(row) {
      public Cell[] call() throws IOException {
        return server.get(location.getRegionInfo().getRegionName(), row, 
          column, timestamp, numVersions);
      }
    });

    if (values != null) {
      ArrayList<Cell> cellValues = new ArrayList<Cell>();
      for (int i = 0 ; i < values.length; i++) {
        cellValues.add(values[i]);
      }
      return cellValues.toArray(new Cell[values.length]);
    }
    return null;
  }
      
  /** 
   * Get all the data for the specified row at the latest timestamp
   * 
   * @param row row key
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public Map<Text, Cell> getRow(final Text row) throws IOException {
    return getRow(row, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get all the data for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param ts timestamp
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public Map<Text, Cell> getRow(final Text row, final long ts) 
  throws IOException {
    return getRegionServerWithRetries(new ServerCallable<RowResult>(row) {
      public RowResult call() throws IOException {
        return server.getRow(location.getRegionInfo().getRegionName(), row, ts);
      }
    });
  }

  /** 
   * Get selected columns for the specified row at the latest timestamp
   * 
   * @param row row key
   * @param columns Array of column names you want to retrieve.
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public Map<Text, Cell> getRow(final Text row, final Text[] columns) 
  throws IOException {
    return getRow(row, columns, HConstants.LATEST_TIMESTAMP);
  }

  /** 
   * Get selected columns for the specified row at a specified timestamp
   * 
   * @param row row key
   * @param columns Array of column names you want to retrieve.   
   * @param ts timestamp
   * @return Map of columns to values.  Map is empty if row does not exist.
   * @throws IOException
   */
  public Map<Text, Cell> getRow(final Text row, final Text[] columns, 
    final long ts) 
  throws IOException {       
    return getRegionServerWithRetries(new ServerCallable<RowResult>(row) {
      public RowResult call() throws IOException {
        return server.getRow(location.getRegionInfo().getRegionName(), row, 
          columns, ts);
      }
    });
  }

  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(Text[] columns, Text startRow)
  throws IOException {
    return getScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(Text[] columns, Text startRow,
    long timestamp)
  throws IOException {
    return getScanner(columns, startRow, timestamp, null);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(Text[] columns, Text startRow,
    RowFilterInterface filter)
  throws IOException { 
    return getScanner(columns, startRow, HConstants.LATEST_TIMESTAMP, filter);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row and
   * ending just before <code>stopRow<code>.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param stopRow Row to stop scanning on. Once we hit this row we stop
   * returning values; i.e. we return the row before this one but not the
   * <code>stopRow</code> itself.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final Text[] columns,
    final Text startRow, final Text stopRow)
  throws IOException {
    return getScanner(columns, startRow, stopRow,
      HConstants.LATEST_TIMESTAMP);
  }
  
  /** 
   * Get a scanner on the current table starting at the specified row and
   * ending just before <code>stopRow<code>.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param stopRow Row to stop scanning on. Once we hit this row we stop
   * returning values; i.e. we return the row before this one but not the
   * <code>stopRow</code> itself.
   * @param timestamp only return results whose timestamp <= this value
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(final Text[] columns,
    final Text startRow, final Text stopRow, final long timestamp)
  throws IOException {
    return getScanner(columns, startRow, timestamp,
      new WhileMatchRowFilter(new StopRowFilter(stopRow)));
  }  
  
  /** 
   * Get a scanner on the current table starting at the specified row.
   * Return the specified columns.
   *
   * @param columns columns to scan. If column name is a column family, all
   * columns of the specified column family are returned.  Its also possible
   * to pass a regex in the column qualifier. A column qualifier is judged to
   * be a regex if it contains at least one of the following characters:
   * <code>\+|^&*$[]]}{)(</code>.
   * @param startRow starting row in table to scan
   * @param timestamp only return results whose timestamp <= this value
   * @param filter a row filter using row-key regexp and/or column data filter.
   * @return scanner
   * @throws IOException
   */
  public Scanner getScanner(Text[] columns,
    Text startRow, long timestamp, RowFilterInterface filter)
  throws IOException {
    return new ClientScanner(columns, startRow, timestamp, filter);
  }

  /** 
   * Delete all cells that match the passed row and column.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @throws IOException 
   */
  public void deleteAll(final Text row, final Text column) throws IOException {
    deleteAll(row, column, LATEST_TIMESTAMP);
  }
  
  /** 
   * Delete all cells that match the passed row and column and whose
   * timestamp is equal-to or older than the passed timestamp.
   * @param row Row to update
   * @param column name of column whose value is to be deleted
   * @param ts Delete all cells of the same timestamp or older.
   * @throws IOException 
   */
  public void deleteAll(final Text row, final Text column, final long ts)
  throws IOException {
    getRegionServerWithRetries(new ServerCallable<Boolean>(row) {
      public Boolean call() throws IOException {
        server.deleteAll(location.getRegionInfo().getRegionName(), row, 
          column, ts);
        return null;
      }
    });
  }
  
  /**
   * Completely delete the row's cells of the same timestamp or older.
   *
   * @param row Key of the row you want to completely delete.
   * @param ts Timestamp of cells to delete
   * @throws IOException
   */
  public void deleteAll(final Text row, final long ts) throws IOException {
    getRegionServerWithRetries(new ServerCallable<Boolean>(row){
      public Boolean call() throws IOException {
        server.deleteAll(location.getRegionInfo().getRegionName(), row, ts);
        return null;
      }
    });
  }
      
  /**
   * Completely delete the row's cells.
   *
   * @param row Key of the row you want to completely delete.
   * @throws IOException
   */
  public void deleteAll(final Text row) throws IOException {
    deleteAll(row, HConstants.LATEST_TIMESTAMP);
  }
  
  /**
   * Delete all cells for a row with matching column family with timestamps
   * less than or equal to <i>timestamp</i>.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @param timestamp Timestamp to match
   * @throws IOException
   */
  public void deleteFamily(final Text row, final Text family, 
    final long timestamp)
  throws IOException {
    getRegionServerWithRetries(new ServerCallable<Boolean>(row){
      public Boolean call() throws IOException {
        server.deleteFamily(location.getRegionInfo().getRegionName(), row, 
          family, timestamp);
        return null;
      }
    });
  }

  /**
   * Delete all cells for a row with matching column family at all timestamps.
   *
   * @param row The row to operate on
   * @param family The column family to match
   * @throws IOException
   */  
  public void deleteFamily(final Text row, final Text family) throws IOException{
    deleteFamily(row, family, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Commit a BatchUpdate to the table.
   * @param batchUpdate
   * @throws IOException
   */ 
  public synchronized void commit(final BatchUpdate batchUpdate) 
  throws IOException {
    getRegionServerWithRetries(
      new ServerCallable<Boolean>(batchUpdate.getRow()){
        public Boolean call() throws IOException {
          server.batchUpdate(location.getRegionInfo().getRegionName(), 
            batchUpdate);
          return null;
        }
      }
    );  
  }
  
  /**
   * Implements the scanner interface for the HBase client.
   * If there are multiple regions in a table, this scanner will iterate
   * through them all.
   */
  protected class ClientScanner implements Scanner {
    private Text[] columns;
    private Text startRow;
    private long scanTime;
    @SuppressWarnings("hiding")
    private boolean closed;
    private HRegionLocation currentRegionLocation;
    private HRegionInterface server;
    private long scannerId;
    private RowFilterInterface filter;
    
    protected ClientScanner(Text[] columns, Text startRow, long timestamp,
      RowFilterInterface filter) 
    throws IOException {

      LOG.debug("Creating scanner over " + tableName + " starting at key " + startRow);

      // defaults
      this.closed = false;
      this.server = null;
      this.scannerId = -1L;
    
      // save off the simple parameters
      this.columns = columns;
      this.startRow = startRow;
      this.scanTime = timestamp;
      
      // save the filter, and make sure that the filter applies to the data
      // we're expecting to pull back
      this.filter = filter;
      if (filter != null) {
        filter.validate(columns);
      }

      nextScanner();
    }
        
    /*
     * Gets a scanner for the next region.
     * Returns false if there are no more scanners.
     */
    private boolean nextScanner() throws IOException {
      // close the previous scanner if it's open
      if (this.scannerId != -1L) {
        this.server.close(this.scannerId);
        this.scannerId = -1L;
      }

      // if we're at the end of the table, then close and return false
      // to stop iterating
      if (currentRegionLocation != null){
        LOG.debug("Advancing forward from region " 
          + currentRegionLocation.getRegionInfo());

        Text endKey = currentRegionLocation.getRegionInfo().getEndKey();
        if (endKey == null || endKey.equals(EMPTY_TEXT)) {
          close();
          return false;
        }
      } 
      
      HRegionLocation oldLocation = this.currentRegionLocation;
      
      Text localStartKey = oldLocation == null ? 
        startRow : oldLocation.getRegionInfo().getEndKey();

      // advance to the region that starts with the current region's end key
      currentRegionLocation = getRegionLocation(localStartKey);

      LOG.debug("Advancing internal scanner to startKey " + localStartKey 
        + ", new region: " + currentRegionLocation);
            
      try {
        for (int tries = 0; tries < numRetries; tries++) {
          // connect to the server
          server = connection.getHRegionConnection(
            this.currentRegionLocation.getServerAddress());
          
          try {
            // open a scanner on the region server starting at the 
            // beginning of the region
            scannerId = server.openScanner(
              this.currentRegionLocation.getRegionInfo().getRegionName(),
              this.columns, localStartKey, scanTime, filter);
              
            break;
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = RemoteExceptionHandler.decodeRemoteException(
                  (RemoteException) e);
            }
            if (tries == numRetries - 1) {
              // No more tries
              throw e;
            }
            try {
              Thread.sleep(pause);
            } catch (InterruptedException ie) {
              // continue
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("reloading table servers because: " + e.getMessage());
            }
            currentRegionLocation = getRegionLocation(localStartKey, true);
          }
        }
      } catch (IOException e) {
        close();
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        throw e;
      }
      return true;
    }

    /** {@inheritDoc} */
    public RowResult next() throws IOException {
      if (this.closed) {
        return null;
      }
      
      RowResult values = null;
      do {
        values = server.next(scannerId);
      } while (values != null && values.size() == 0 && nextScanner());

      if (values != null && values.size() != 0) {
        return values;
      }
      
      return null;
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
      if (scannerId != -1L) {
        try {
          server.close(scannerId);
        } catch (IOException e) {
          // We used to catch this error, interpret, and rethrow. However, we
          // have since decided that it's not nice for a scanner's close to
          // throw exceptions. Chances are it was just an UnknownScanner
          // exception due to lease time out.
        }
        scannerId = -1L;
      }
      server = null;
      closed = true;
    }

    /** {@inheritDoc} */
    public Iterator<RowResult> iterator() {
      return new Iterator<RowResult>() {
        // The next RowResult, possibly pre-read
        RowResult next = null;
        
        // return true if there is another item pending, false if there isn't.
        // this method is where the actual advancing takes place, but you need
        // to call next() to consume it. hasNext() will only advance if there
        // isn't a pending next().
        public boolean hasNext() {
          if (next == null) {
            try {
              next = ClientScanner.this.next();
              return next != null;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }            
          }
          return true;
        }

        // get the pending next item and advance the iterator. returns null if
        // there is no next item.
        public RowResult next() {
          // since hasNext() does the real advancing, we call this to determine
          // if there is a next before proceeding.
          if (!hasNext()) {
            return null;
          }
          
          // if we get to here, then hasNext() has given us an item to return.
          // we want to return the item and then null out the next pointer, so
          // we use a temporary variable.
          RowResult temp = next;
          next = null;
          return temp;
        }

        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
  
  /**
   * Inherits from Callable, used to define the particular actions you would
   * like to take with retry logic.
   */
  protected abstract class ServerCallable<T> implements Callable<T> {
    HRegionLocation location;
    HRegionInterface server;
    Text row;
  
    protected ServerCallable(Text row) {
      this.row = row;
    }
  
    void instantiateServer(boolean reload) throws IOException {
      this.location = getRegionLocation(row, reload);
      this.server = connection.getHRegionConnection(location.getServerAddress());
    }    
  }
  
  /**
   * Pass in a ServerCallable with your particular bit of logic defined and 
   * this method will manage the process of doing retries with timed waits 
   * and refinds of missing regions.
   */
  protected <T> T getRegionServerWithRetries(ServerCallable<T> callable) 
  throws IOException, RuntimeException {
    List<IOException> exceptions = new ArrayList<IOException>();
    for(int tries = 0; tries < numRetries; tries++) {
      try {
        callable.instantiateServer(tries != 0);
        return callable.call();
      } catch (IOException e) {
        if (e instanceof RemoteException) {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        }
        if (tries == numRetries - 1) {
          if (LOG.isDebugEnabled()) {
            String message = "Trying to contact region server for row '" + 
              callable.row + "', but failed after " + (tries + 1)  + 
              " attempts.\n";
            int i = 1;
            for (IOException e2 : exceptions) {
              message = message + "Exception " + i++ + ":\n" + e2;
            }
            LOG.debug(message);
          }
          throw e;
        }
        if (LOG.isDebugEnabled()) {
          exceptions.add(e);
          LOG.debug("reloading table servers because: " + e.getMessage());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    return null;    
  }
}
