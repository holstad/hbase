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

import java.io.Writer;

import org.apache.hadoop.hbase.HBaseConfiguration;

public class ExitCommand extends BasicCommand {
  public ExitCommand(Writer o) {
    super(o);
  }

  public ReturnMsg execute(@SuppressWarnings("unused")
  HBaseConfiguration conf) {
    // TOD: Is this the best way to exit? Would be a problem if shell is run
    // inside another program -- St.Ack 09/11/2007
    System.exit(9999);
    return null;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.SHELL;
  }
}
