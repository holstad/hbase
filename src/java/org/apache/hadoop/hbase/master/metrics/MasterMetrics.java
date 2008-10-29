/**
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
package org.apache.hadoop.hbase.master.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;


/** 
 * This class is for maintaining the various master statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class MasterMetrics implements Updater {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private final MetricsRecord metricsRecord;
  
  /*
   * Count of requests to the cluster since last call to metrics update
   */
  private final MetricsIntValue cluster_requests =
    new MetricsIntValue("cluster_requests");

  public MasterMetrics() {
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = MetricsUtil.createRecord(context, "master");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("Master", name);
    context.registerUpdater(this);
    JvmMetrics.init("Master", name);
    LOG.info("Initialized");
  }
  
  public void shutdown() {
    // nought to do.
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(@SuppressWarnings("unused") MetricsContext unused) {
    synchronized (this) {
      synchronized(this.cluster_requests) {
        this.cluster_requests.pushMetric(metricsRecord);
        // Set requests down to zero again.
        this.cluster_requests.set(0);
      }
    }
    this.metricsRecord.update();
  }
  
  public void resetAllMinMax() {
    // Nothing to do
  }

  /**
   * @return Count of requests.
   */
  public int getRequests() {
    return this.cluster_requests.get();
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    synchronized(this.cluster_requests) {
      this.cluster_requests.inc(inc);
    }
  }
}