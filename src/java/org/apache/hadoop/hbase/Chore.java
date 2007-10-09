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
package org.apache.hadoop.hbase;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Sleeper;

/**
 * Chore is a task performed on a period in hbase.  The chore is run in its own
 * thread. This base abstract class provides while loop and sleeping facility.
 * If an unhandled exception, the threads exit is logged.
 * Implementers just need to add checking if there is work to be done and if
 * so, do it.  Its the base of most of the chore threads in hbase.
 */
public abstract class Chore extends Thread {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final Sleeper sleeper;
  protected final AtomicBoolean stop;
  
  /**
   * @param p Period at which we should run.  Will be adjusted appropriately
   * should we find work and it takes time to complete.
   * @param s When this flag is set to true, this thread will cleanup and exit
   * cleanly.
   */
  public Chore(final int p, final AtomicBoolean s) {
    super();
    this.sleeper = new Sleeper(p, s);
    this.stop = s;
  }

  public void run() {
    try {
      initialChore();
      this.sleeper.sleep();
      while(!this.stop.get()) {
        long startTime = System.currentTimeMillis();
        chore();
        this.sleeper.sleep(startTime);
      }
    } finally {
      LOG.info(getName() + " exiting");
    }
  }
  
  /**
   * Override to run a task before we start looping.
   */
  protected void initialChore() {
    // Default does nothing.
  }
  
  /**
   * Look for chores.  If any found, do them else just return.
   */
  protected abstract void chore();

  /**
   * Sleep for period.
   */
  protected void sleep() {
    this.sleeper.sleep();
  }
}