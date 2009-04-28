package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Family;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestAbstractServerGet extends TestCase {
  
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
  
  private boolean multiFamily = false;
  
  private final int KEY_OFFSET = 2*Bytes.SIZEOF_INT;

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


//  private String toString(byte[] bytes) {
//    String ret = "";
//    for(int i=0;i<bytes.length;i++) {
//      ret += String.format("%x ",bytes[i]);
//    }
//    return ret;
//  }
  
  public void testIsDelete(){
    
  }
  
  
  public void testIsDeleted(){
    //Build KeyValue to check
    //Using putKv2
    
    //Build list of deletes
    List<KeyValue> deletes = new ArrayList<KeyValue>();
    deletes.add(delKv1);
    deletes.add(delKv2);
    
    //Test
    sget.setDeletes(deletes);
    byte[] currBytes = putKv2.getBuffer();
    int initCurrOffset = putKv2.getOffset();
    short currRowLen = putKv2.getRowLength();
//    int columnoffset = putKvs2.get
    byte currFamLen = getFamilyLength(putKv2);
    int currColLen = getColumnLength(putKv2, currRowLen, currFamLen);
    
    int ret = sget.isDeleted(currBytes, initCurrOffset, currRowLen, currFamLen,
        currColLen, multiFamily);
    assertTrue(ret != 0);
//    assertEquals(1, ret);
    //Add deleteFamily
    
    //Test
    
  } 
  
  
  public void testCompareDeletesToMerge(){
    
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
    sget.setDeletes(l1);
    sget.setNewDeletes(l2);
    
    //merge lists
    List<KeyValue> mergedDeletes = null;
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
      assertSame(key, delKv3);
      if(PRINT) System.out.println("key " +key);
    }
    
    //merge lists
    if(PRINT) System.out.println();
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
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
    
    sget.setDeletes(l1);
    sget.setNewDeletes(l2);
    
    //merge lists
    List<KeyValue> mergedDeletes = null;
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
      assertSame(key, delKv4);
      if(PRINT){
        System.out.println("key " +key);
      }
    }
    
    //merge lists
    if(PRINT) System.out.println();
    sget.setDeletes(l2);
    sget.setNewDeletes(l1);
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
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
    sget.setDeletes(l1);
    sget.setNewDeletes(l2);
    
    //merge lists
    List<KeyValue> mergedDeletes = null;
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
      assertSame(key, delKv41);
      if(PRINT) System.out.println("key " +key);
    }
    
    //merge lists
    if(PRINT) System.out.println();
    sget.setDeletes(l2);
    sget.setNewDeletes(l1);
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    for(KeyValue key : mergedDeletes){
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
    
    sget.setDeletes(l1);
    sget.setNewDeletes(l2);
    
    KeyValue [] resultArr = new KeyValue[3];
    resultArr[0] = newKey1;
    resultArr[1] = oldKey2;
    resultArr[2] = newKey3;
    
    //merge lists
    List<KeyValue> mergedDeletes = null;
    sget.mergeDeletes(multiFamily);
    mergedDeletes = sget.getDeletes();
    
    //check result
    int i = 0;
    for(KeyValue key : mergedDeletes){
      assertSame(key, resultArr[i++]);
      if(PRINT) System.out.println("key " +key);
    }
    
  }
  
  
  public void testMergeGets(){
    
  }
  
  
  //Helpers
  
  //Extra methods used only for testing
  public byte getFamilyLength(KeyValue kv){
    short rowLen = kv.getRowLength();
    return kv.getBuffer()[KEY_OFFSET + Bytes.SIZEOF_SHORT + rowLen];
  }
  public int getColumnLength(KeyValue kv, short rowLen, byte famLen){
    return (kv.getKeyLength() - (Bytes.SIZEOF_SHORT + rowLen +
        Bytes.SIZEOF_BYTE + famLen + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_LONG));
  }
  
  private void printList(List list){
    for(Object o : list){
      System.out.println(o);
    }
    System.out.println();
  }

}
