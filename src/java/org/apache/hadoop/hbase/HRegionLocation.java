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

/**
 * Contains the HRegionInfo for the region and the HServerAddress for the
 * HRegionServer serving the region
 */
@SuppressWarnings("unchecked")
public class HRegionLocation implements Comparable {
  private HRegionInfo regionInfo;
  private HServerAddress serverAddress;

  /**
   * Constructor
   * 
   * @param regionInfo the HRegionInfo for the region
   * @param serverAddress the HServerAddress for the region server
   */
  public HRegionLocation(HRegionInfo regionInfo, HServerAddress serverAddress) {
    this.regionInfo = regionInfo;
    this.serverAddress = serverAddress;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "address: " + this.serverAddress.toString() + ", regioninfo: " +
      this.regionInfo;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    return this.compareTo(o) == 0;
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int result = this.regionInfo.hashCode();
    result ^= this.serverAddress.hashCode();
    return result;
  }
  
  /** @return HRegionInfo */
  public HRegionInfo getRegionInfo(){
    return regionInfo;
  }

  /** @return HServerAddress */
  public HServerAddress getServerAddress(){
    return serverAddress;
  }

  //
  // Comparable
  //
  
  /**
   * {@inheritDoc}
   */
  public int compareTo(Object o) {
    HRegionLocation other = (HRegionLocation) o;
    int result = this.regionInfo.compareTo(other.regionInfo);
    if(result == 0) {
      result = this.serverAddress.compareTo(other.serverAddress);
    }
    return result;
  }
}
