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
package org.apache.hadoop.hbase.shell.algebra;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

/**
 * Operation interface for one of algebra operations like relational algebra
 * operations, matrix algebra operations, linear algebra operations, topological
 * algebra operations, etc.
 */
public interface Operation {

  /**
   * return the Map/Reduce job configuration for performing operations.
   * 
   * @return JobConf
   * @throws IOException
   * @throws RuntimeException
   */
  JobConf getConf() throws IOException, RuntimeException;
}
