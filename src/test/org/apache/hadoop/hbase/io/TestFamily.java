package org.apache.hadoop.hbase.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


import junit.framework.TestCase;

public class TestFamily extends TestCase {
  private final boolean PRINT = false;
  
  private byte[] row = "row1".getBytes();
  private long ts = System.currentTimeMillis();
  private byte[] familyName = "fam1".getBytes();
  private byte[] col1 = "col1".getBytes();
  private byte[] col2 = "col2".getBytes();
  private byte[] col3 = "col3".getBytes();

  List<byte[]> columns = new ArrayList<byte[]>();
  Family family = null;
  

  protected void setUp() throws Exception{
    super.setUp();
    
    columns.add(col2);
    columns.add(col1);
    columns.add(col3);
    
    family = new Family(familyName, columns);
  }
  
  public void testSorted(){
    if(PRINT){
      System.out.println(family.toString());
    }
    family.sortColumns();
    List<byte[]> sortedCols = family.getColumns();
    for(int i=0; i<sortedCols.size()-1; i++){
      assertTrue(Bytes.compareTo(sortedCols.get(i), sortedCols.get(i+1)) < 0);
    }
    if(PRINT){
      System.out.println(family.toString());
    }
  }
  
  public void testKeyValueCreated(){
    byte[] val1 = "val1".getBytes();
    KeyValue.Type type = KeyValue.Type.Put;
    
    Family family1 = new Family(familyName, col1, val1);
    KeyValue kvComp = new KeyValue(row, familyName, col1, ts, type, val1);
    
    family1.createKeyValuesFromColumns(row, ts);
    byte[] kvBytes = family1.getColumns().get(0);
    int res = Bytes.compareTo(kvBytes, kvComp.getBuffer());
    assertEquals(0, res);
  }
  
}
