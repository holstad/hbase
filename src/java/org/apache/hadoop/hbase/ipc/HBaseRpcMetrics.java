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
package org.apache.hadoop.hbase.ipc;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 * for example:
 *  <p> {@link #rpcQueueTime}.inc(time)
 *
 */
public class HBaseRpcMetrics implements Updater {
  private MetricsRecord metricsRecord;
  private static Log LOG = LogFactory.getLog(HBaseRpcMetrics.class);
  
  public HBaseRpcMetrics(String hostName, String port, HBaseServer server) {
    MetricsContext context = MetricsUtil.getContext("rpc");
    metricsRecord = MetricsUtil.createRecord(context, "metrics");

    metricsRecord.setTag("port", port);

    LOG.info("Initializing RPC Metrics with hostName=" 
        + hostName + ", port=" + port);

    context.registerUpdater(this);
  }
  
  
  /**
   * The metrics variables are public:
   *  - they can be set directly by calling their set/inc methods
   *  -they can also be read directly - e.g. JMX does this.
   */
  
  public MetricsTimeVaryingRate rpcQueueTime = new MetricsTimeVaryingRate("RpcQueueTime");
  public MetricsTimeVaryingRate rpcProcessingTime = new MetricsTimeVaryingRate("RpcProcessingTime");

  public Map <String, MetricsTimeVaryingRate> metricsList = Collections.synchronizedMap(new HashMap<String, MetricsTimeVaryingRate>());

  
  
  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   */
  public void doUpdates(MetricsContext context) {
    rpcQueueTime.pushMetric(metricsRecord);
    rpcProcessingTime.pushMetric(metricsRecord);

    synchronized (metricsList) {
	// Iterate through the rpcMetrics hashmap to propogate the different rpc metrics.
	Set keys = metricsList.keySet();

	Iterator keyIter = keys.iterator();

	while (keyIter.hasNext()) {
		Object key = keyIter.next();
		MetricsTimeVaryingRate value = metricsList.get(key);

		value.pushMetric(metricsRecord);
	}
    }
    metricsRecord.update();
  }

  public void shutdown() {
    // Nothing to do
  }
}