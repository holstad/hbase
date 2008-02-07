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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

/**
 * Leases
 *
 * There are several server classes in HBase that need to track external
 * clients that occasionally send heartbeats.
 * 
 * <p>These external clients hold resources in the server class.
 * Those resources need to be released if the external client fails to send a
 * heartbeat after some interval of time passes.
 *
 * <p>The Leases class is a general reusable class for this kind of pattern.
 * An instance of the Leases class will create a thread to do its dirty work.  
 * You should close() the instance if you want to clean up the thread properly.
 */
public class Leases extends Thread {
  private static final Log LOG = LogFactory.getLog(Leases.class.getName());
  private final int leasePeriod;
  private final int leaseCheckFrequency;
  private volatile DelayQueue<Lease> leaseQueue = new DelayQueue<Lease>();

  protected final Map<String, Lease> leases = new HashMap<String, Lease>();
  protected final Map<String, LeaseListener> listeners =
    new HashMap<String, LeaseListener>();
  private volatile boolean stopRequested = false;

  /**
   * Creates a lease monitor
   * 
   * @param leasePeriod - length of time (milliseconds) that the lease is valid
   * @param leaseCheckFrequency - how often the lease should be checked
   * (milliseconds)
   */
  public Leases(final int leasePeriod, final int leaseCheckFrequency) {
    this.leasePeriod = leasePeriod;
    this.leaseCheckFrequency = leaseCheckFrequency;
  }
  
  /** {@inheritDoc} */
  @Override
  public void run() {
    while (!stopRequested || (stopRequested && leaseQueue.size() > 0) ) {
      Lease lease = null;
      try {
        lease = leaseQueue.poll(leaseCheckFrequency, TimeUnit.MILLISECONDS);
        
      } catch (InterruptedException e) {
        continue;
        
      } catch (ConcurrentModificationException e) {
        continue;
      }
      if (lease == null) {
        continue;
      }
      // A lease expired
      LeaseListener listener = null;
      synchronized (leaseQueue) {
        String leaseName = lease.getLeaseName();
        leases.remove(leaseName);
        listener = listeners.remove(leaseName);
        if (listener == null) {
          LOG.error("lease listener is null for lease " + leaseName);
          continue;
        }
      }
      listener.leaseExpired();
    }
    close();
  }

  /**
   * Shuts down this lease instance when all outstanding leases expire.
   * Like {@link #close()} but rather than violently end all leases, waits
   * first on extant leases to finish.  Use this method if the lease holders
   * could loose data, leak locks, etc.  Presumes client has shutdown
   * allocation of new leases.
   */
  public void closeAfterLeasesExpire() {
    this.stopRequested = true;
  }
  
  /**
   * Shut down this Leases instance.  All pending leases will be destroyed, 
   * without any cancellation calls.
   */
  public void close() {
    LOG.info(Thread.currentThread().getName() + " closing leases");
    this.stopRequested = true;
    synchronized (leaseQueue) {
      leaseQueue.clear();
      leases.clear();
      listeners.clear();
      leaseQueue.notifyAll();
    }
    LOG.info(Thread.currentThread().getName() + " closed leases");
  }

  /**
   * Obtain a lease
   * 
   * @param leaseName name of the lease
   * @param listener listener that will process lease expirations
   */
  public void createLease(String leaseName, final LeaseListener listener) {
    if (stopRequested) {
      return;
    }
    Lease lease = new Lease(leaseName, System.currentTimeMillis() + leasePeriod);
    synchronized (leaseQueue) {
      if (leases.containsKey(leaseName)) {
        throw new IllegalStateException("lease '" + leaseName +
            "' already exists");
      }
      leases.put(leaseName, lease);
      listeners.put(leaseName, listener);
      leaseQueue.add(lease);
    }
  }
  
  /**
   * Renew a lease
   * 
   * @param leaseName name of lease
   */
  public void renewLease(final String leaseName) {
    synchronized (leaseQueue) {
      Lease lease = leases.get(leaseName);
      if (lease == null) {
        throw new IllegalArgumentException("lease '" + leaseName +
            "' does not exist");
      }
      leaseQueue.remove(lease);
      lease.setExpirationTime(System.currentTimeMillis() + leasePeriod);
      leaseQueue.add(lease);
    }
  }

  /**
   * Client explicitly cancels a lease.
   * 
   * @param leaseName name of lease
   */
  public void cancelLease(final String leaseName) {
    synchronized (leaseQueue) {
      Lease lease = leases.remove(leaseName);
      if (lease == null) {
        throw new IllegalArgumentException("lease '" + leaseName +
            "' does not exist");
      }
      leaseQueue.remove(lease);
      listeners.remove(leaseName);
    }
  }

  /** This class tracks a single Lease. */
  private static class Lease implements Delayed {
    private final String leaseName;
    private long expirationTime;

    Lease(final String leaseName, long expirationTime) {
      this.leaseName = leaseName;
      this.expirationTime = expirationTime;
    }

    /** @return the lease name */
    public String getLeaseName() {
      return leaseName;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      return this.hashCode() == ((Lease) obj).hashCode();
    }
    
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return this.leaseName.hashCode();
    }

    /** {@inheritDoc} */
    public long getDelay(TimeUnit unit) {
      return unit.convert(this.expirationTime - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
    }

    /** {@inheritDoc} */
    public int compareTo(Delayed o) {
      long delta = this.getDelay(TimeUnit.MILLISECONDS) -
      o.getDelay(TimeUnit.MILLISECONDS);

      int value = 0;
      if (delta > 0) {
        value = 1;

      } else if (delta < 0) {
        value = -1;
      }
      return value;
    }

    /** @param expirationTime the expirationTime to set */
    public void setExpirationTime(long expirationTime) {
      this.expirationTime = expirationTime;
    }
  }
}