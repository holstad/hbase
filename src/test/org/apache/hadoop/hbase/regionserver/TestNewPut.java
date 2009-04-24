package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Family;
import org.apache.hadoop.hbase.io.RowUpdates;

import junit.framework.TestCase;

public class TestNewPut extends HBaseTestCase implements HConstants  {
  
  private static byte[] row = "row1".getBytes();
  private static byte[] fam = "fam1:".getBytes();
  private static byte[] col1 = "col1".getBytes();
  private static byte[] col2 = "col2".getBytes();
  private static byte[] col3 = "col3".getBytes();
  private static byte[] col4 = "col4".getBytes();
  private static byte[] col5 = "col5".getBytes();
  
  private static byte[] val1 = "val1".getBytes();
  private static byte[] val2 = "val2".getBytes();
  private static byte[] val3 = "val3".getBytes();
  private static byte[] val4 = "val4".getBytes();
  private static byte[] val5 = "val5".getBytes();
  
  private static byte[] column1 = "fam1:col1".getBytes();
  private static byte[] column2 = "fam1:col2".getBytes();
  private static byte[] column3 = "fam1:col3".getBytes();
  private static byte[] column4 = "fam1:col4".getBytes();
  private static byte[] column5 = "fam1:col5".getBytes();
  
  private static List<byte[]> columns = null;
  
  private HRegion region = null;
  private RowUpdates updates = null;
  
  Integer lockid = null;
  
//  boolean writeToWAL = true;
  boolean writeToWAL = false;
  Family family = null;

  long ts = 0L;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    HTableDescriptor htd = new HTableDescriptor(getName());
    htd.addFamily(new HColumnDescriptor(fam));

    region = createNewHRegion(htd, null, null);
    
//    columns = new ArrayList<byte[]>();
//    columns.add(col1);
//    columns.add(col2);
//    columns.add(col3);
//    columns.add(col4);
//    columns.add(col5);
    family = new Family(fam, col1, val1);
    family.add(col2, val2);
    family.add(col3, val3);
    family.add(col4, val4);
    family.add(col5, val5);
    
    ts = System.currentTimeMillis();
    updates = new RowUpdates(row, family, ts);
    updates.createKeyValuesFromColumns();
//    lockid = -1;
  }
  
  public void testPutRowUpdate() throws IOException {
    //Create a RowUpdate
    long start = 0L;
    long stop = 0L;
    start = System.nanoTime();

    System.out.println("start " +start);
    region.updateRow(updates, lockid, writeToWAL);
    stop = System.nanoTime();

    System.out.println("stop  " +stop);

    System.out.println("timer " +(stop-start));

    System.out.println("\n");

//    store-mem
    
    BatchUpdate bu = new BatchUpdate(row);
    bu.put(column1, val1);
    bu.put(column2, val2);
    bu.put(column3, val3);
    bu.put(column4, val4);
    bu.put(column5, val5);
    
    start = System.nanoTime();
    System.out.println("start " +start);
    region.batchUpdate(bu, lockid, writeToWAL);
    stop = System.nanoTime();
    System.out.println("stop  " +stop);
    System.out.println("timer " +(stop-start));
//    region.updateRow(RowUpdates rups, Integer lockid, boolean writeToWAL);
//    region.newUpdate(byte [] family, List<byte[]> bss, boolean writeToWAL)
    
  }

}
