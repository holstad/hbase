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

import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConnection;
import org.apache.hadoop.hbase.HConnectionManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;

/**
 * Creates tables.
 */
public class CreateCommand extends SchemaModificationCommand {
  private Text tableName;
  private Map<String, Map<String, Object>> columnSpecMap =
    new HashMap<String, Map<String, Object>>();
  
  public CreateCommand(Writer o) {
    super(o);
  }
  
  public ReturnMsg execute(HBaseConfiguration conf) {
    try {
      HConnection conn = HConnectionManager.getConnection(conf);
      if (conn.tableExists(this.tableName)) {
        return new ReturnMsg(0, "'" + this.tableName + "' Table already exist");
      }
      
      HBaseAdmin admin = new HBaseAdmin(conf);
      HTableDescriptor tableDesc = new HTableDescriptor(tableName.toString());
      HColumnDescriptor columnDesc = null;
      Set<String> columns = columnSpecMap.keySet();
      for (String column : columns) {
        columnDesc = getColumnDescriptor(column, columnSpecMap.get(column));
        tableDesc.addFamily(columnDesc);
      }
      
      println("Creating table... Please wait.");
      
      admin.createTable(tableDesc);
      return new ReturnMsg(0, "Table created successfully.");
    }
    catch (Exception e) {
      return new ReturnMsg(0, extractErrMsg(e));
    }
  }

  /**
   * Sets the table to be created.
   * @param table Table to be created
   */
  public void setTable(String tableName) {
    this.tableName = new Text(tableName);
  }

  /**
   * Adds a column specification.  
   * @param columnSpec Column specification
   */
  public void addColumnSpec(String column, Map<String, Object> columnSpec) {
    columnSpecMap.put(column, columnSpec);
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.DDL;
  }
}