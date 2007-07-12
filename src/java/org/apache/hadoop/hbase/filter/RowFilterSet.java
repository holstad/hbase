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
package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

/**
 * Implementation of RowFilterInterface that represents a set of RowFilters
 * which will be evaluated with a specified boolean operator AND/OR. Since you
 * can use RowFilterSets as children of RowFilterSet, you can create a
 * hierarchy of filters to be evaluated.
 */
public class RowFilterSet implements RowFilterInterface {

  enum Operator {
    AND, OR
  }

  private Operator operator = Operator.AND;
  private Set<RowFilterInterface> filters = new HashSet<RowFilterInterface>();

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public RowFilterSet() {
    super();
  }

  /**
   * Constructor that takes a set of RowFilters. The default operator AND is
   * assumed.
   * 
   * @param rowFilters
   */
  public RowFilterSet(final Set<RowFilterInterface> rowFilters) {
    this.filters = rowFilters;
  }

  /**
   * Constructor that takes a set of RowFilters and an operator.
   * 
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public RowFilterSet(final Operator operator,
      final Set<RowFilterInterface> rowFilters) {
    this.filters = rowFilters;
    this.operator = operator;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void validate(final Text[] columns) {
    for (RowFilterInterface filter : filters) {
      filter.validate(columns);
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void reset() {
    for (RowFilterInterface filter : filters) {
      filter.reset();
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void acceptedRow(final Text key) {
    for (RowFilterInterface filter : filters) {
      filter.acceptedRow(key);
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterAllRemaining() {
    boolean result = operator == Operator.OR;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.AND) {
        if (filter.filterAllRemaining()) {
          return true;
        }
      } else if (operator == Operator.OR) {
        if (!filter.filterAllRemaining()) {
          return false;
        }
      }
    }
    return result;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(final Text rowKey) {
    boolean result = operator == Operator.OR;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.AND) {
        if (filter.filterAllRemaining() || filter.filter(rowKey)) {
          return true;
        }
      } else if (operator == Operator.OR) {
        if (!filter.filterAllRemaining() && !filter.filter(rowKey)) {
          return false;
        }
      }
    }
    return result;

  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filter(final Text rowKey, final Text colKey, final byte[] data) {
    boolean result = operator == Operator.OR;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.AND) {
        if (filter.filterAllRemaining() || filter.filter(rowKey, colKey, data)) {
          return true;
        }
      } else if (operator == Operator.OR) {
        if (!filter.filterAllRemaining()
            && !filter.filter(rowKey, colKey, data)) {
          return false;
        }
      }
    }
    return result;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public boolean filterNotNull(final TreeMap<Text, byte[]> columns) {
    boolean result = operator == Operator.OR;
    for (RowFilterInterface filter : filters) {
      if (operator == Operator.AND) {
        if (filter.filterAllRemaining() || filter.filterNotNull(columns)) {
          return true;
        }
      } else if (operator == Operator.OR) {
        if (!filter.filterAllRemaining() && !filter.filterNotNull(columns)) {
          return false;
        }
      }
    }
    return result;
  }

  /**
   * 
   * {@inheritDoc}
   */
  public void readFields(final DataInput in) throws IOException {
    byte opByte = in.readByte();
    operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      filters = new HashSet<RowFilterInterface>();
      try {
        for (int i = 0; i < size; i++) {
          String className = in.readUTF();
          Class<?> clazz = Class.forName(className);
          RowFilterInterface filter;
          filter = (RowFilterInterface) clazz.newInstance();
          filter.readFields(in);
          filters.add(filter);
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to deserialize RowFilterInterface.",
            e);
      }
    }

  }

  /**
   * 
   * {@inheritDoc}
   */
  public void write(final DataOutput out) throws IOException {
    out.writeByte(operator.ordinal());
    out.writeInt(filters.size());
    for (RowFilterInterface filter : filters) {
      out.writeUTF(filter.getClass().getName());
      filter.write(out);
    }
  }

}
