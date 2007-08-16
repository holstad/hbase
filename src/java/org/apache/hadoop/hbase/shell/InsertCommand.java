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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTable;
import org.apache.hadoop.io.Text;

public class InsertCommand extends BasicCommand {
  
  private Text table;
  private List<String> columnfamilies;
  private List<String> values;
  private Map<String, List<String>> condition;

  public ReturnMsg execute(Configuration conf) {
    if (this.table == null || this.values == null || this.condition == null)
      return new ReturnMsg(0, "Syntax error : Please check 'Insert' syntax.");

    if (this.columnfamilies.size() != this.values.size())
      return new ReturnMsg(0,
          "Mismatch between values list and columnfamilies list");

    try {
      HTable table = new HTable(conf, this.table);
      long lockId = table.startUpdate(getRow());

      for (int i = 0; i < this.values.size(); i++) {
        table.put(lockId, getColumn(i), getValue(i));
        
      }
      table.commit(lockId);

      return new ReturnMsg(1, "1 row inserted successfully.");
    } catch (IOException e) {
      String[] msg = e.getMessage().split("[\n]");
      return new ReturnMsg(0, msg[0]);
    }
  }

  public void setTable(String table) {
    this.table = new Text(table);
  }

  public void setColumnfamilies(List<String> columnfamilies) {
    this.columnfamilies = columnfamilies;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public void setCondition(Map<String, List<String>> cond) {
    this.condition = cond;
  }

  public Text getRow() {
    return new Text(this.condition.get("row").get(1));
  }

  public Text getColumn(int i) {
    return new Text(this.columnfamilies.get(i));
  }

  public byte[] getValue(int i) {
    return this.values.get(i).getBytes();
  }
}
