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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;


import junit.framework.TestCase;

/**
 * Tests filter sets
 */
public class TestRowFilterSet extends TestCase {

  RowFilterInterface filterMPALL;
  RowFilterInterface filterMPONE;
  static final int MAX_PAGES = 5;
  final char FIRST_CHAR = 'a';
  final char LAST_CHAR = 'e';
  final byte[] GOOD_BYTES = "abc".getBytes();
  final byte[] BAD_BYTES = "def".getBytes();
  TreeMap<Text, byte[]> colvalues;

  /** {@inheritDoc} */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    
    colvalues = new TreeMap<Text, byte[]>();
    for (char c = FIRST_CHAR; c < LAST_CHAR; c++) {
      colvalues.put(new Text(new String(new char [] {c})), GOOD_BYTES);
    }
    
    Set<RowFilterInterface> filters = new HashSet<RowFilterInterface>();
    filters.add(new PageRowFilter(MAX_PAGES));
    filters.add(new RegExpRowFilter(".*regex.*", colvalues));
    filters.add(new WhileMatchRowFilter(new StopRowFilter(new Text("yyy"))));
    filters.add(new WhileMatchRowFilter(new RegExpRowFilter(".*match.*")));
    filterMPALL = new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ALL, 
      filters);
    filterMPONE = new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ONE, 
      filters);
  }
  
  /**
   * Test "must pass one"
   * @throws Exception
   */
  public void testMPONE() throws Exception {
    MPONETests(filterMPONE);
  }

  /**
   * Test "must pass all"
   * @throws Exception
   */
  public void testMPALL() throws Exception {
    MPALLTests(filterMPALL);
  }
  
  /**
   * Test serialization
   * @throws Exception
   */
  public void testSerialization() throws Exception {
    // Decompose filterMPALL to bytes.
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    filterMPALL.write(out);
    out.close();
    byte[] buffer = stream.toByteArray();
    
    // Recompose filterMPALL.
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(buffer));
    RowFilterInterface newFilter = new RowFilterSet();
    newFilter.readFields(in);
    
    // Ensure the serialization preserved the filter by running a full test.
    MPALLTests(newFilter);
  }
  
  private void MPONETests(RowFilterInterface filter) throws Exception {
    // A row that shouldn't cause any filters to return true.
    RFSAssertion(filter, "regex_match", false);
    
    // A row that should cause the WhileMatchRowFilter to filter all remaining.
    RFSAssertion(filter, "regex_only", false);
    
    // Make sure the overall filterAllRemaining is unchanged (correct for 
    // MUST_PASS_ONE).
    assertFalse(filter.filterAllRemaining());
    
    // A row that should cause the RegExpRowFilter to fail and the 
    // StopRowFilter to filter all remaining.
    RFSAssertion(filter, "yyy_match", false);
    
    // Accept several more rows such that PageRowFilter will exceed its limit.
    for (int i=0; i<=MAX_PAGES-3; i++)
      filter.rowProcessed(false, new Text("unimportant_key"));
    
    // A row that should cause the RegExpRowFilter to filter this row, making 
    // all the filters return true and thus the RowFilterSet as well.
    RFSAssertion(filter, "bad_column", true);
    
    // Make sure the overall filterAllRemaining is unchanged (correct for 
    // MUST_PASS_ONE).
    assertFalse(filter.filterAllRemaining());
  }
  
  private void MPALLTests(RowFilterInterface filter) throws Exception {
    // A row that shouldn't cause any filters to return true.
    RFSAssertion(filter, "regex_match", false);
    
    // A row that should cause WhileMatchRowFilter to filter all remaining.
    RFSAssertion(filter, "regex_only", true);

    // Make sure the overall filterAllRemaining is changed (correct for 
    // MUST_PASS_ALL).
    RFSAssertReset(filter);
    
    // A row that should cause the RegExpRowFilter to fail and the 
    // StopRowFilter to filter all remaining.
    RFSAssertion(filter, "yyy_match", true);

    // Make sure the overall filterAllRemaining is changed (correct for 
    // MUST_PASS_ALL).
    RFSAssertReset(filter);
    
    // A row that should cause the RegExpRowFilter to fail.
    boolean filtered = filter.filter(new Text("regex_match"), 
      new Text(new String(new char[] { FIRST_CHAR })), BAD_BYTES);
    assertTrue("Filtering on 'regex_match' and bad column data.", filtered);
    filterMPALL.rowProcessed(filtered, new Text("regex_match"));
  }
  
  private void RFSAssertion(RowFilterInterface filter, String toTest, 
    boolean assertTrue) throws Exception {
    Text testText = new Text(toTest);
    boolean filtered = filter.filter(testText);
    assertTrue("Filtering on '" + toTest + "'", 
      assertTrue? filtered : !filtered);
    filter.rowProcessed(filtered, testText);
  }
  
  private void RFSAssertReset(RowFilterInterface filter) throws Exception{
    assertTrue(filter.filterAllRemaining());
    // Reset for continued testing
    filter.reset();
    assertFalse(filter.filterAllRemaining());
  }
}
