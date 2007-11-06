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
package org.apache.hadoop.hbase.shell;

import java.io.IOException;
import java.io.Writer;

import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Clears the console screen. 
 */
public class ClearCommand extends BasicCommand {
  public ClearCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(@SuppressWarnings("unused") HBaseConfiguration conf) {
    clear();
    return null;
  }

  private void clear() {
    String osName = System.getProperty("os.name");
    if (osName.length() > 7 && osName.subSequence(0, 7).equals("Windows")) {
      try {
        Runtime.getRuntime().exec("cmd /C cls");
      } catch (IOException e) {
        try {
          println("Can't clear." + e.toString());
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
    } else {
      try {
        print("\033c");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}