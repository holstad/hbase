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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * HServerInfo contains metainfo about an HRegionServer, Currently it only
 * contains the server start code.
 * 
 * In the future it will contain information about the source machine and
 * load statistics.
 */
public class HServerInfo implements WritableComparable<HServerInfo> {
  private HServerAddress serverAddress;
  private long startCode;
  private HServerLoad load;
  private int infoPort;
  private transient volatile String serverName = null;

  /** default constructor - used by Writable */
  public HServerInfo() {
    this(new HServerAddress(), 0, HConstants.DEFAULT_REGIONSERVER_INFOPORT);
  }
  
  /**
   * Constructor
   * @param serverAddress
   * @param startCode
   * @param infoPort Port the info server is listening on.
   */
  public HServerInfo(HServerAddress serverAddress, long startCode,
      final int infoPort) {
    this.serverAddress = serverAddress;
    this.startCode = startCode;
    this.load = new HServerLoad();
    this.infoPort = infoPort;
  }
  
  /**
   * Construct a new object using another as input (like a copy constructor)
   * @param other
   */
  public HServerInfo(HServerInfo other) {
    this.serverAddress = new HServerAddress(other.getServerAddress());
    this.startCode = other.getStartCode();
    this.load = other.getLoad();
    this.infoPort = other.getInfoPort();
  }
  
  /**
   * @return the load
   */
  public HServerLoad getLoad() {
    return load;
  }

  /**
   * @param load the load to set
   */
  public void setLoad(HServerLoad load) {
    this.load = load;
  }

  /** @return the server address */
  public synchronized HServerAddress getServerAddress() {
    return new HServerAddress(serverAddress);
  }
  
  /**
   * Change the server address.
   * @param serverAddress New server address
   */
  public synchronized void setServerAddress(HServerAddress serverAddress) {
    this.serverAddress = serverAddress;
    this.serverName = null;
  }
 
  /** @return the server start code */
  public synchronized long getStartCode() {
    return startCode;
  }
  
  /**
   * @return Port the info server is listening on.
   */
  public int getInfoPort() {
    return this.infoPort;
  }
  
  /**
   * @param startCode the startCode to set
   */
  public synchronized void setStartCode(long startCode) {
    this.startCode = startCode;
    this.serverName = null;
  }
  
  /**
   * @return the server name in the form hostname_startcode_port
   */
  public synchronized String getServerName() {
    if (this.serverName == null) {
      this.serverName = getServerName(this.serverAddress, this.startCode);
    }
    return this.serverName;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "address: " + this.serverAddress + ", startcode: " + this.startCode
    + ", load: (" + this.load.toString() + ")";
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((HServerInfo)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.getServerName().hashCode();
  }


  // Writable
  
  public void readFields(DataInput in) throws IOException {
    this.serverAddress.readFields(in);
    this.startCode = in.readLong();
    this.load.readFields(in);
    this.infoPort = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    this.serverAddress.write(out);
    out.writeLong(this.startCode);
    this.load.write(out);
    out.writeInt(this.infoPort);
  }

  public int compareTo(HServerInfo o) {
    return this.getServerName().compareTo(o.getServerName());
  }

  /**
   * @param info
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(HServerInfo info) {
    return getServerName(info.getServerAddress(), info.getStartCode());
  }
  
  /**
   * @param serverAddress in the form hostname:port
   * @param startCode
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(String serverAddress, long startCode) {
    String name = null;
    if (serverAddress != null) {
      HServerAddress address = new HServerAddress(serverAddress);
      name = getServerName(address.getHostname(), address.getPort(), startCode);
    }
    return name;
  }

  /**
   * @param address
   * @param startCode
   * @return the server name in the form hostname_startcode_port
   */
  public static String getServerName(HServerAddress address, long startCode) {
    return getServerName(address.getHostname(), address.getPort(), startCode);
  }

  private static String getServerName(String hostName, int port, long startCode) {
    StringBuilder name = new StringBuilder(hostName);
    name.append("_");
    name.append(startCode);
    name.append("_");
    name.append(port);
    return name.toString();
  }
}
