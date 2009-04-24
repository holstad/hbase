package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetFamilies;
import org.apache.hadoop.hbase.io.TimeRange;
import junit.framework.TestCase;

public class TestServerGetFamilies extends TestCase {
  
  private final boolean PRINT = false;
  
  byte [] row1 = null;
  byte [] fam1 = null;
  byte [] col1 = null;
  byte [] val1 = null;

  byte [] row2 = null;
  byte [] fam2 = null;
  byte [] col2 = null;
  byte [] val2 = null;
  
  byte [] col3 = null;
  byte [] col4 = null;
  byte [] col5 = null;
  byte [] col6 = null;
  
  
  KeyValue putKv1 = null;
  KeyValue putKv2 = null;
  KeyValue putKv3 = null;
  KeyValue putKv4 = null;
  KeyValue putKv5 = null;
  KeyValue putKv6 = null;
  
  short versionsToFetch = 0;
  long ts = 0L;
  
  TimeRange tr = null;
  Get get = null;
  ServerGet sget = null;
  
  boolean multiFamily = false;
  
  protected void setUp() throws Exception {
    super.setUp();
    ts = System.currentTimeMillis();
    
    row1 = "row1".getBytes();
    fam1 = "fam1".getBytes();
    col1 = "col1".getBytes();
    val1 = "val1".getBytes();
    
    row2 = "row2".getBytes();
    fam2 = "fam2".getBytes();
    col2 = "col2".getBytes();
    val2 = "val2".getBytes();
    
    col3 = "col3".getBytes();
    col4 = "col4".getBytes();
    col5 = "col5".getBytes();
    col6 = "col6".getBytes();
    
    putKv1 = new KeyValue(row1, fam1, col1, ts, KeyValue.Type.Put, val1);
    putKv2 = new KeyValue(row1, fam1, col2, ts, KeyValue.Type.Put, val2);
    putKv3 = new KeyValue(row1, fam1, col3, ts, KeyValue.Type.Put, val1);
    putKv4 = new KeyValue(row1, fam1, col4, ts, KeyValue.Type.Put, val2);
    putKv5 = new KeyValue(row1, fam1, col5, ts, KeyValue.Type.Put, val1);
    putKv6 = new KeyValue(row2, fam1, col6, ts, KeyValue.Type.Put, val2);
    
    tr = new TimeRange();
    get = new GetFamilies(row1, fam1, versionsToFetch, tr);
    
    sget = new ServerGetFamilies(get);
    sget.setFamily(fam1);
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
  }

  
  public void testCompareColumn()
  throws IOException{
    List<KeyValue> list = new ArrayList<KeyValue>();
    list.add(putKv1);
    list.add(putKv2);
    
    int res = 0;
    for(int i=0; i< list.size(); i++){
      res = sget.compareTo(list.get(i), multiFamily);
      assertEquals(1, res);
    }

  }
  
  public void testUpdateVersions(){
    
  }
  
  public void testCompareTo(){
    
  }
  
  public void testMergeGets()
  throws IOException{
    List<KeyValue> list = new ArrayList<KeyValue>();
    list.add(putKv1);
    list.add(putKv2);
    
    int res = 0;
    for(int i=0; i< list.size(); i++){
      res = sget.compareTo(list.get(i), multiFamily);
      assertEquals(1, res);
//      System.out.println("res " +res);
    }
    
//    System.out.println("newColumns.size " +((ServerGetFamilies)sget).getNewColumns().size());

//    ((ServerGetFamilies)sget).mergeGets(multiFamily);
//    System.out.println("mergedList.size " +mergedList.size());
    list.clear();
    list.add(putKv3);
    list.add(putKv4);
    
    for(int i=0; i< list.size(); i++){
      res = sget.compareTo(list.get(i), multiFamily);
//      assertEquals(1, res);
//      System.out.println("res " +res);
    }
    
//    ((ServerGetFamilies)sget).mergeGets(multiFamily);

  }
}
