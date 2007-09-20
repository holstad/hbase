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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.dfs.DistributedFileSystem;

/**
 * Utility methods for interacting with the underlying file system.
 */
public class FSUtils {
  private static final Log LOG = LogFactory.getLog(FSUtils.class);

  /**
   * Not instantiable
   */
  private FSUtils() {}
  
  /**
   * Checks to see if the specified file system is available
   * 
   * @param fs
   * @return true if the specified file system is available.
   */
  public static boolean isFileSystemAvailable(FileSystem fs) {
    if (!(fs instanceof DistributedFileSystem)) {
      return true;
    }
    boolean available = false;
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    int maxTries = dfs.getConf().getInt("hbase.client.retries.number", 3);
    Path root = new Path(dfs.getConf().get("hbase.dir", "/"));
    for (int i = 0; i < maxTries; i++) {
      IOException ex = null;
      try {
        if (dfs.exists(root)) {
          available = true;
          break;
        }
      } catch (IOException e) {
        ex = e;
      }
      String exception = "";
      if (ex != null) {
        exception = ": " + ex.getMessage();
      }
      LOG.info("Failed exists test on " + root + " (Attempt " + i + ")" +
          exception);
    }
    try {
      if (!available) {
        fs.close();
      }
        
    } catch (IOException e) {
        LOG.error("file system close failed: ", e);
    }
    return available;
  }
}
