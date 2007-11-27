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
package org.apache.hadoop.hbase.shell;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HScannerInterface;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.shell.generated.Parser;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Text;

/**
 * Selects values from tables.
 * 
 * TODO: INTO FILE is not yet implemented.
 */
public class SelectCommand extends BasicCommand {
  private Text tableName;
  private Text rowKey = new Text("");
  private List<String> columns;
  private long timestamp;
  private int limit;
  // Count of versions to return.
  private int version;
  private boolean whereClause = false;
  private static final String [] HEADER_ROW_CELL =
    new String [] {"Row", "Cell"};
  private static final String [] HEADER_COLUMN_CELL =
    new String [] {"Column", "Cell"};
  private static final String [] HEADER =
    new String [] {"Row", "Column", "Cell"};
  private static final String STAR = "*";
  
  private final TableFormatter formatter;
  
  // Not instantiable
  @SuppressWarnings("unused")
  private SelectCommand() {
    this(null, null);
  }
  
  public SelectCommand(final Writer o, final TableFormatter f) {
    super(o);
    this.formatter = f;
  }

  public ReturnMsg execute(final HBaseConfiguration conf) {
    if (this.tableName.equals("") || this.rowKey == null ||
        this.columns.size() == 0) {
      return new ReturnMsg(0, "Syntax error : Please check 'Select' syntax.");
    } 
    try {
      HConnection conn = HConnectionManager.getConnection(conf);
      if (!conn.tableExists(this.tableName)) {
        return new ReturnMsg(0, "'" + this.tableName + "' Table not found");
      }
      
      HTable table = new HTable(conf, this.tableName);
      HBaseAdmin admin = new HBaseAdmin(conf);
      int count = 0;
      if (this.whereClause) {
        count = compoundWherePrint(table, admin);
      } else {
        count = scanPrint(table, admin);
      }
      return new ReturnMsg(1, Integer.toString(count) + " row(s) in set");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[,]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  private int compoundWherePrint(HTable table, HBaseAdmin admin) {
    int count = 0;
    try {
      if (this.version != 0) {
        // A number of versions has been specified.
        byte[][] result = null;
        ParsedColumns parsedColumns = getColumns(admin, false);
        boolean multiple = parsedColumns.isMultiple() || this.version > 1;
        formatter.header(multiple? HEADER_COLUMN_CELL: null);
        for (Text column: parsedColumns.getColumns()) {
          if (this.timestamp != 0) {
            result = table.get(this.rowKey, column, this.timestamp,
              this.version);
          } else {
            result = table.get(this.rowKey, column, this.version);
          }
          for (int ii = 0; result != null && ii < result.length; ii++) {
            if (multiple) {
              formatter.row(new String [] {column.toString(),
                toString(column, result[ii])});
            } else {
              formatter.row(new String [] {toString(column, result[ii])});
            }
            count++;
          }
        }
      } else {
        formatter.header(isMultiple()? HEADER_COLUMN_CELL: null);
        for (Map.Entry<Text, byte[]> e: table.getRow(this.rowKey).entrySet()) {
          Text key = e.getKey();
          String keyStr = key.toString();
          if (!this.columns.contains(STAR) && !this.columns.contains(keyStr)) {
            continue;
          }
          String cellData = toString(key, e.getValue());
          if (isMultiple()) {
            formatter.row(new String [] {key.toString(), cellData});
          } else {
            formatter.row(new String [] {cellData});
          }
          count++;
        }
      }
      formatter.footer();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return count;
  }
  
  private String toString(final Text columnName, final byte [] cell)
  throws IOException {
    String result = null;
    if (columnName.equals(HConstants.COL_REGIONINFO) ||
        columnName.equals(HConstants.COL_SPLITA) ||
        columnName.equals(HConstants.COL_SPLITA)) {
      result = Writables.getHRegionInfoOrNull(cell).toString();
    } else if (columnName.equals(HConstants.COL_STARTCODE)) {
      result = Long.toString(Writables.bytesToLong(cell));
    } else {
      result = Writables.bytesToString(cell);
    }
    return result;
  }
  
  /**
   * Data structure with columns to use scanning and whether or not the
   * scan could return more than one column.
   */
  class ParsedColumns {
    private final List<Text> cols;
    private final boolean isMultiple;
    
    ParsedColumns(final List<Text> columns) {
      this(columns, true);
    }
    
    ParsedColumns(final List<Text> columns, final boolean isMultiple) {
      this.cols = columns;
      this.isMultiple = isMultiple;
    }
    
    public List<Text> getColumns() {
      return this.cols;
    }
    
    public boolean isMultiple() {
      return this.isMultiple;
    }
  }
  
  private int scanPrint(HTable table,
      HBaseAdmin admin) {
    int count = 0;
    HScannerInterface scan = null;
    try {
      ParsedColumns parsedColumns = getColumns(admin, true);
      Text [] cols = parsedColumns.getColumns().toArray(new Text [] {});
      if (this.timestamp == 0) {
        scan = table.obtainScanner(cols, this.rowKey);
      } else {
        scan = table.obtainScanner(cols, this.rowKey, this.timestamp);
      }
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      // If only one column in query, then don't print out the column.
      formatter.header((parsedColumns.isMultiple())? HEADER: HEADER_ROW_CELL);
      while (scan.next(key, results) && checkLimit(count)) {
        Text r = key.getRow();
        for (Text columnKey : results.keySet()) {
          String cellData = toString(columnKey, results.get(columnKey));
          if (parsedColumns.isMultiple()) {
            formatter.row(new String [] {r.toString(), columnKey.toString(),
              cellData});
          } else {
            // Don't print out the column since only one specified in query.
            formatter.row(new String [] {r.toString(), cellData});
          }
          count++;
          if (this.limit > 0 && count >= this.limit) {
            break;
          }
        }
        // Clear results else subsequent results polluted w/ previous finds.
        results.clear();
      }
      formatter.footer();
      scan.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return count;
  }

  /**
   * Make sense of the supplied list of columns.
   * @param admin Admin to use.
   * @return Interpretation of supplied list of columns.
   */
  public ParsedColumns getColumns(final HBaseAdmin admin,
      final boolean scanning) {
    ParsedColumns result = null;
    try {
      if (this.columns.contains("*")) {
        if (this.tableName.equals(HConstants.ROOT_TABLE_NAME)
            || this.tableName.equals(HConstants.META_TABLE_NAME)) {
          result =
            new ParsedColumns(Arrays.asList(HConstants.COLUMN_FAMILY_ARRAY));
        } else {
          HTableDescriptor[] tables = admin.listTables();
          for (int i = 0; i < tables.length; i++) {
            if (tables[i].getName().equals(this.tableName)) {
              result = new ParsedColumns(new ArrayList<Text>(tables[i].
                families().keySet()));
              break;
            }
          }
        }
      } else {
        List<Text> tmpList = new ArrayList<Text>();
        for (int i = 0; i < this.columns.size(); i++) {
          Text column = null;
          // Add '$' to column name if we are scanning.  Scanners support
          // regex column names.  Adding '$', the column becomes a
          // regex that does an explicit match on the supplied column name.
          // Otherwise, if the specified column is a column family, then
          // default behavior is to fetch all columns that have a matching
          // column family.
          column = (this.columns.get(i).contains(":"))?
            new Text(this.columns.get(i) + (scanning? "$": "")):
            new Text(this.columns.get(i) + ":" + (scanning? "$": ""));
          tmpList.add(column);
        }
        result = new ParsedColumns(tmpList, tmpList.size() > 1);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return result;
  }
  
  /*
   * @return True if query contains multiple columns.
   */
  private boolean isMultiple() {
    return this.columns.size() > 1 || this.columns.contains(STAR);
  }

  private boolean checkLimit(int count) {
    return (this.limit == 0)? true: (this.limit > count) ? true : false;
  }

  public void setTable(String table) {
    this.tableName = new Text(table);
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setWhere(boolean isWhereClause) {
    if (isWhereClause)
      this.whereClause = true;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = Long.parseLong(timestamp);
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public void setRowKey(String rowKey) {
    if(rowKey == null) 
      this.rowKey = null; 
    else
      this.rowKey = new Text(rowKey);
  }

  /**
   * @param version Set maximum versions for this selection
   */
  public void setVersion(int version) {
    this.version = version;
  }
  
  public static void main(String[] args) throws Exception {
    Writer out = new OutputStreamWriter(System.out, "UTF-8");
    HBaseConfiguration c = new HBaseConfiguration();
    // For debugging
    TableFormatterFactory tff =
      new TableFormatterFactory(out, c);
    Parser parser = new Parser("select * from 'x' where row='x';", out,  tff.get());
    Command cmd = parser.terminatedCommand();
    
    ReturnMsg rm = cmd.execute(c);
    out.write(rm == null? "": rm.toString());
    out.flush();
  }
}
