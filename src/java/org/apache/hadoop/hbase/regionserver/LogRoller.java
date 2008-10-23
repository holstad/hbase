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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.RemoteExceptionHandler;

/** Runs periodically to determine if the HLog should be rolled */
class LogRoller extends Thread implements LogRollListener {
  static final Log LOG = LogFactory.getLog(LogRoller.class);  
  private final ReentrantLock rollLock = new ReentrantLock();
  private final AtomicBoolean rollLog = new AtomicBoolean(false);
  private final HRegionServer server;
  
  /** @param server */
  public LogRoller(final HRegionServer server) {
    super();
    this.server = server;
  }

  @Override
  public void run() {
    while (!server.isStopRequested()) {
      if (!rollLog.get()) {
        synchronized (rollLog) {
          try {
            rollLog.wait(server.threadWakeFrequency);
          } catch (InterruptedException e) {
            continue;
          }
        }
        continue;
      }
      rollLock.lock();          // Don't interrupt us. We're working
      try {
        LOG.info("Rolling hlog. Number of entries: " +
            server.getLog().getNumEntries());
        server.getLog().rollWriter();
      } catch (FailedLogCloseException e) {
        LOG.fatal("Forcing server shutdown", e);
        server.abort();
      } catch (IOException ex) {
        LOG.error("Log rolling failed with ioe: ",
            RemoteExceptionHandler.checkIOException(ex));
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Log rolling failed", ex);
        server.checkFileSystem();
      } finally {
        rollLog.set(false);
        rollLock.unlock();
      }
    }
    LOG.info("LogRoller exiting.");
  }

  public void logRollRequested() {
    synchronized (rollLog) {
      rollLog.set(true);
      rollLog.notifyAll();
    }
  }
  
  /**
   * Called by region server to wake up this thread if it sleeping.
   * It is sleeping if rollLock is not held.
   */
  public void interruptIfNecessary() {
    try {
      rollLock.lock();
      this.interrupt();
    } finally {
      rollLock.unlock();
    }
  }
}