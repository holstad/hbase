package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.Family;
import org.apache.hadoop.hbase.io.TimeRange;


import junit.framework.TestCase;

public class TestServerGetColumns extends TestCase {
  
  private final boolean PRINT = false;
  byte [] row1 = null;
  byte [] fam1 = null;
  byte [] col1 = null;
  byte [] val1 = null;

  byte [] row2 = null;
  byte [] fam2 = null;
  byte [] col2 = null;
  byte [] val2 = null;
  
  byte [] fam3 = null;
  byte [] col3 = null;
  byte [] val3 = null;

  byte [] fam4 = null;
  byte [] col4 = null;
  byte [] val4 = null;
  
  List<Family> families1 = null;
  long ts = 0L;
  long ts1 = 0L;
  long ts2 = 0L;
  long ts3 = 0L;
  short versionsToFetch = 0;

  KeyValue putKv1 = null;
  KeyValue putKv2 = null;
  KeyValue putKv3 = null;
  KeyValue putKv4 = null;
  KeyValue putKv5 = null;
  
  KeyValue delKv1 = null;
  KeyValue delKv2 = null;
  KeyValue delKv21 = null;
  KeyValue delKv3 = null;
  KeyValue delKv4 = null;
  KeyValue delKv41 = null;
  KeyValue delKv5 = null;
  
  Get get = null;
  ServerGet sget = null;
  
  TimeRange tr = null;

  protected void setUp() throws Exception {
    super.setUp();
    row1 = "row1".getBytes();
    fam1 = "fam1".getBytes();
    col1 = "col1".getBytes();
    val1 = "val1".getBytes();
    
    row2 = "row2".getBytes();
    fam2 = "fam2".getBytes();
    col2 = "col2".getBytes();
    val2 = "val2".getBytes();
    
    fam3 = "fam3".getBytes();
    col3 = "col3".getBytes();
    val3 = "val3".getBytes();
    
    fam4 = "fam4".getBytes();
    col4 = "col4".getBytes();
    val4 = "val4".getBytes();
    
    families1 = new ArrayList<Family>();
    families1.add(new Family(fam1, col1));
    ts1 = System.nanoTime();
    versionsToFetch = 1;
    
    putKv1 = new KeyValue(row1, fam1, col1, ts1, KeyValue.Type.Put, val1);
    putKv2 = new KeyValue(row1, fam1, col2, ts1, KeyValue.Type.Put, val2);
    putKv3 = new KeyValue(row2, fam1, col2, ts1, KeyValue.Type.Put, val2);
    
    delKv1 = new KeyValue(row1, fam1, col1, ts1, KeyValue.Type.Delete, val1);
    delKv2 = new KeyValue(row1, fam1, col2, ts1, KeyValue.Type.Delete, val1);
    delKv21 = new KeyValue(row1, fam1, col2, ts1, KeyValue.Type.DeleteColumn, val1);

    
    ts2 = System.nanoTime();
    putKv4 = new KeyValue(row1, fam1, col1, ts2, KeyValue.Type.Put, val1);
    delKv3 = new KeyValue(row1, fam1, col1, ts2, KeyValue.Type.Delete, val1);
    delKv4 = new KeyValue(row1, fam1, col2, ts1, KeyValue.Type.DeleteColumn, val1);
    delKv41 = new KeyValue(row1, fam1, col2, ts2, KeyValue.Type.DeleteColumn, val1);
   
    ts3 = System.nanoTime();
    putKv5 = new KeyValue(row1, fam1, col1, ts3, KeyValue.Type.Put, val1);
    delKv5 = new KeyValue(row1, fam1, col3, ts3, KeyValue.Type.Delete, val1);
    
    Get get = new GetColumns(row1, families1, versionsToFetch, ts1);
    sget = new ServerGetColumns(get);
    sget.setFamily(families1.get(0).getFamily());
    sget.setColumns(families1.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
  }


  private String toString(byte[] bytes) {
    String ret = "";
    for(int i=0;i<bytes.length;i++) {
      ret += String.format("%x ",bytes[i]);
    }
    return ret;
  }
  
  //Key
  public void testUpdateKeyValue(){
    byte [] row1 = "row1".getBytes();
    byte [] fam1 = "fam1:".getBytes();
    
    KeyValue ok = new KeyValue(row1, fam1);
    KeyValue k = new KeyValue(ok);
    
    updateKey(k);
    assertNotSame(ok, k);
  }
  private void updateKey(KeyValue k){
    byte [] row = "row2".getBytes();
    byte [] fam = "fam2:".getBytes();
    k.set(new KeyValue(row, fam));
  }

  
  //ServerGetColumns
  public void testCompare_equal()
  throws IOException{
    //Compare
    int ret = sget.compareTo(putKv1, false);

    //1 means that they were the same and that it should be added
    assertEquals(1, ret);
  }

  public void testCompare_notEqual()
  throws IOException{
    //Compare
    int ret = sget.compareTo(putKv2, false);

    //1 means that they were the same and that it should be added
    assertEquals(2, ret);
  }
 
  public void testCompare_multiColumns()
  throws IOException{
    //Create a ServerGet object
    byte [] col3 = "col3".getBytes();
    
    Family family = new Family(fam1, col1);
    family.add(col3);
    
    List<Family> families = new ArrayList<Family>();
    families.add(family);
    
    get = new GetColumns(row1, families, versionsToFetch, ts1);
    sget = new ServerGetColumns(get);
    sget.setFamily(families.get(0).getFamily());
    sget.setColumns(families.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
    
    //Create a KeyValue object
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv1);
    kvs.add(putKv2);
    
    Iterator<KeyValue> iter = kvs.iterator();
    //Compare
    int ret = 0;
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT){
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
        } else if(ret == 3){
          System.out.println("done!!");
        }
          System.out.println("fetching next value from store\n");
      }
    }
    assertEquals(0, ret);
  }
  
  public void testCompare_deleteList()
  throws IOException{
    //Create a ServerGet object
    byte [] col2 = "col2".getBytes();
    byte [] col3 = "col3".getBytes();
    
    Family family = new Family(fam1, col1);
    family.add(col2);
    family.add(col3);
    
    List<Family> families = new ArrayList<Family>();
    families.add(family);
    
    get = new GetColumns(row1, families, versionsToFetch, ts1);
    sget = new ServerGetColumns(get);
    sget.setFamily(families.get(0).getFamily());
    sget.setColumns(families.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
    
    //Create a KeyValue object
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv1);
    kvs.add(putKv2);
    
    //Creating a deleteList
    List<KeyValue> deletes = new ArrayList<KeyValue>();
    deletes.add(delKv1);
    
    //Adding deleteList to serverGet
    sget.setDeletes(deletes);
    
    Iterator<KeyValue> iter = kvs.iterator();
    //Compare
    int ret = 0;
    int found = 0;
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT){
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
        } else if(ret == 3){
          System.out.println("done!!");
        }
        System.out.println("fetching next value from store\n");
      }
      found += ret;
    }
    if(PRINT) System.out.println("found " +found);
    assertEquals(1, found);
    
  }
  
  public void testNextStoreFile()
  throws IOException{
    //Create a ServerGet object
    byte [] col2 = "col2".getBytes();
    byte [] col3 = "col3".getBytes();
    
    Family family = new Family(fam1, col1);
    family.add(col2);
    family.add(col3);
    
    List<Family> families = new ArrayList<Family>();
    families.add(family);
    
    get = new GetColumns(row1, families, versionsToFetch, ts);
    sget = new ServerGetColumns(get);
    sget.setFamily(families.get(0).getFamily());
    sget.setColumns(families.get(0).getColumns());
    
    //Create a KeyValue object
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv1);
    kvs.add(putKv3);
    
    Iterator<KeyValue> iter = kvs.iterator();
    //Compare
    int ret = 0;
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT){
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
        System.out.println("fetching next value from store\n");
      }
    }
    if(PRINT){
      System.out.println("ret " +ret);
    }
    assertEquals(2, ret);
  }
  
  
  
  public void testDone()
  throws IOException{
    //Create a KeyValue object
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv1);
    kvs.add(putKv2);
    kvs.add(putKv3);
    
    Iterator<KeyValue> iter = kvs.iterator();
    //Compare
    int ret = 0;
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT){
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
        System.out.println("fetching next value from store\n");
      }
    }
    if(PRINT){
      System.out.println("ret " +ret);
    }
    assertEquals(3, ret);
  }

  public void testMergeDeletes_Delete_Delete(){
    //Create delete keyLists to merge
    List<KeyValue> l1 = new ArrayList<KeyValue>();
    List<KeyValue> l2 = new ArrayList<KeyValue>();
    
    l1.add(delKv1);
    if(PRINT){
      printList(l1);
    }
    
    l2.add(delKv3);
    if(PRINT){
      printList(l2);
    }
    
    //merge lists
    Deletes mergedDeletes = null;
    mergedDeletes = sget.mergeDeletes(l1, l2);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv3);
      if(PRINT) System.out.println("key " +key);
    }
    
    //merge lists
    if(PRINT) System.out.println();
    mergedDeletes = sget.mergeDeletes(l2, l1);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv3);
      if(PRINT) System.out.println("key " +key);
    }
  }

  
  public void testMergeDeletes_Delete_DeleteColumn(){
    //Create delete keyLists to merge
    List<KeyValue> l1 = new ArrayList<KeyValue>();
    List<KeyValue> l2 = new ArrayList<KeyValue>();
    
    l1.add(delKv2);
    if(PRINT){
      printList(l1);
    }
    
    l2.add(delKv4);
    if(PRINT){
      printList(l2);
    }
    
    //merge lists
    Deletes mergedDeletes = null;
    mergedDeletes = sget.mergeDeletes(l1, l2);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv4);
      if(PRINT){
        System.out.println("key " +key);
      }
    }
    
    //merge lists
    if(PRINT) System.out.println();
    mergedDeletes = sget.mergeDeletes(l2, l1);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv4);
      if(PRINT){
        System.out.println("key " +key);
      }
    }
  }
  

  public void testMergeDeletes_DeleteColumn_DeleteColumn(){
    //Create delete keyLists to merge
    List<KeyValue> l1 = new ArrayList<KeyValue>();
    List<KeyValue> l2 = new ArrayList<KeyValue>();
    
    l1.add(delKv21);
    if(PRINT){
      printList(l1);
    }
    
    l2.add(delKv41);
    if(PRINT){
      printList(l2);
    }
    
    //merge lists
    Deletes mergedDeletes = null;
    mergedDeletes = sget.mergeDeletes(l1, l2);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv41);
      if(PRINT) System.out.println("key " +key);
    }
    
    //merge lists
    if(PRINT) System.out.println();
    mergedDeletes = sget.mergeDeletes(l2, l1);
    
    //check result
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, delKv41);
      if(PRINT) System.out.println("key " +key);
    }
  }  
  
  
  public void testMergeDeletes(){
    //Create delete keyLists to merge
    List<KeyValue> l1 = new ArrayList<KeyValue>();
    List<KeyValue> l2 = new ArrayList<KeyValue>();
    
    KeyValue oldKey1 = delKv1;
    KeyValue oldKey2 = delKv4;
    l1.add(oldKey1);
    l1.add(oldKey2);
    if(PRINT){
      printList(l1);
    }
    
    KeyValue newKey1 = delKv3;
    KeyValue newKey2 = delKv2;
    KeyValue newKey3 = delKv5;
    l2.add(newKey1);
    l2.add(newKey2);
    l2.add(newKey3);
    if(PRINT){
      printList(l2);
    }
    
    KeyValue [] resultArr = new KeyValue[3];
    resultArr[0] = newKey1;
    resultArr[1] = oldKey2;
    resultArr[2] = newKey3;
    
    //merge lists
    Deletes mergedDeletes = sget.mergeDeletes(l1, l2);
    
    //check result
    int i = 0;
    for(KeyValue key : mergedDeletes.getDeletes()){
      assertSame(key, resultArr[i++]);
      if(PRINT) System.out.println("key " +key);
    }
    
  }
  
  
  public void testCompare_updateVersions()
  throws IOException{
    versionsToFetch = 4;
    ts = System.nanoTime();
    tr = new TimeRange(ts, ts1);
    get = new GetColumns(row1, families1, versionsToFetch, tr);
    sget = new ServerGetColumns(get);
    sget.setFamily(families1.get(0).getFamily());
    sget.setColumns(families1.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
    
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv5);
    kvs.add(putKv4);
    kvs.add(putKv1);
    
    Iterator<KeyValue> iter = kvs.iterator();
    
    //Compare
    int ret = 0; 
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT) {
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
      }
      if(PRINT) System.out.println("fetching next value from store\n");
    }

    List<Short> versions = sget.getVersions();
    for(short version : versions){
      if(PRINT) System.out.println("versions fetched " +version);
      assertEquals(3, version);
    }
  }
  
  public void testCompare_timeRange()
  throws IOException{
    versionsToFetch = 4;
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(putKv5);
    kvs.add(putKv4);
    kvs.add(putKv1);
    
    Iterator<KeyValue> iter = null;
    
    int ret = 0;
    
    List<byte[]> columns = null;
    
    int len = 0;
    
    //Getting 1 value
    tr = new TimeRange(ts2, ts1);
    get = new GetColumns(row1, families1, versionsToFetch, tr);
    sget = new ServerGetColumns(get);
    sget.setFamily(families1.get(0).getFamily());
    sget.setColumns(families1.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);

    iter = kvs.iterator();
    
    //Compare
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT) {
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
      }
      if(PRINT) System.out.println("fetching next value from store\n");
    }
    
    List<Short> versions = sget.getVersions();
    for(short version : versions){
      if(PRINT) System.out.println("versions fetched " +version);
      assertEquals(1, version);
    }
    
    //Getting 2 value
    tr = new TimeRange(ts3, ts1);
    Family family = new Family(fam1, col1);
    families1 = new ArrayList<Family>();
    families1.add(family);
//    families1 = new Family[]{new Family(fam1, col1)};

    get = new GetColumns(row1, families1, versionsToFetch, tr);
    sget = new ServerGetColumns(get);
    sget.setFamily(families1.get(0).getFamily());
    sget.setColumns(families1.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
    
    iter = kvs.iterator();
    
    //Compare
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT) {
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
      }
      if(PRINT) System.out.println("fetching next value from store\n");
    }
    
    versions = sget.getVersions();
    for(short version : versions){
      if(PRINT) System.out.println("versions fetched " +version);
      assertEquals(2, version);
    }  
    
    
    //Getting 3 value
    ts = System.nanoTime();
    tr = new TimeRange(ts, ts1);
//    families1 = new Family[]{new Family(fam1, col1)};
    get = new GetColumns(row1, families1, versionsToFetch, tr);
    sget = new ServerGetColumns(get);
    sget.setFamily(families1.get(0).getFamily());
    sget.setColumns(families1.get(0).getColumns());
    sget.setNow(System.nanoTime());
    sget.setTTL(-1L);
    
    iter = kvs.iterator();
    
    //Compare
    while(iter.hasNext()){
      ret = sget.compareTo(iter.next(), false);
      if(PRINT) {
        if(ret == 1){
          System.out.println("adding to result");
        } else if(ret == 2){
          System.out.println("skipping to next store");
          break;
        } else if(ret == 3){
          System.out.println("done!!");
          break;
        }
      }
      if(PRINT) System.out.println("fetching next value from store\n");
    }
    
    versions = sget.getVersions();
    for(short version : versions){
      if(PRINT) System.out.println("versions fetched " +version);
      assertEquals(3, version);
    }
  }
  
  
  private void printList(List list){
    for(Object o : list){
      System.out.println(o);
    }
    System.out.println();
  }
  
}
