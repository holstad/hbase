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
package org.apache.hadoop.hbase.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Scan an HBase table to sort by a specified sort column.
 * If the column does not exist, the record is not passed to Reduce.
 *
 */
@SuppressWarnings("unchecked")
public abstract class TableMap<K extends WritableComparable, V extends Writable>
    extends MapReduceBase implements Mapper<HStoreKey, MapWritable, K, V> {
  /**
   * Use this before submitting a TableMap job. It will
   * appropriately set up the JobConf.
   * 
   * @param table table name
   * @param columns columns to scan
   * @param mapper mapper class
   * @param job job configuration
   */
  public static void initJob(String table, String columns,
      Class<? extends TableMap> mapper, JobConf job) {
    job.setInputFormat(TableInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    job.setMapperClass(mapper);
    job.setInputPath(new Path(table));
    job.set(TableInputFormat.COLUMN_LIST, columns);
  }

  /**
   * Call a user defined function on a single HBase record, represented
   * by a key and its associated record value.
   * 
   * @param key
   * @param value
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void map(HStoreKey key, MapWritable value,
      OutputCollector<K, V> output, Reporter reporter) throws IOException;
}
