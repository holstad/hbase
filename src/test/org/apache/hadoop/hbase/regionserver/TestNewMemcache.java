package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;
import java.util.Comparator;

public class TestNewMemcache extends TestCase {
  private final boolean PRINT = true;

  private Memcache memcache;

  private boolean multiFamily;
  private KeyValue.KVComparator kvComparator;

  private byte [] putRow1 = "row1".getBytes();//114,111,119,49,102,97,109,49
  private byte [] putRow2 = "row2".getBytes();
  private byte [] putFam1 = "fam1".getBytes();
  private byte [] putFam2 = "fam2".getBytes();
  private byte [] putCol1 = "col1".getBytes();
  private byte [] putCol2 = "col2".getBytes();

  private byte [] putVal1 = "val1".getBytes();
  private long putTs = 0L;
  
  private KeyValue put1;
  private KeyValue put2;
  private KeyValue put3;
  private KeyValue put4;
  private KeyValue del1;
  private KeyValue delCol1;
  private KeyValue delFam1;


  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.kvComparator = KeyValue.COMPARATOR;
    this.memcache = new Memcache(HConstants.FOREVER, kvComparator);
    this.multiFamily = false;
    this.putTs = System.nanoTime();
    
    this.put1 = new KeyValue(putRow1, putFam1, putCol1, putTs,
      KeyValue.Type.Put, putVal1);
    this.put2 = new KeyValue(putRow1, putFam2, putCol2, putTs,
      KeyValue.Type.Put, putVal1);
    this.put3 = new KeyValue(putRow2, putFam1, putCol1, putTs,
      KeyValue.Type.Put, putVal1);    
    
    this.del1 = new KeyValue(putRow1, putFam1, putCol1, putTs,
      KeyValue.Type.Delete, putVal1);
    
    this.putTs = System.nanoTime();
    this.delCol1 = new KeyValue(putRow1, putFam1, putCol1, putTs,
      KeyValue.Type.DeleteColumn, putVal1);
    this.put4 = new KeyValue(putRow2, putFam1, putCol1, putTs,
        KeyValue.Type.Put, putVal1);
    
    
    this.putTs = System.nanoTime();
    this.delFam1 = new KeyValue(putRow1, putFam1, putCol1, putTs,
      KeyValue.Type.DeleteFamily, putVal1);
  }
  
  public void testComparator(){
    int ret = 0;
    ret = kvComparator.compare(put2, put3);
    if(PRINT){
      System.out.println("ret " +ret);
    }
    assertTrue(ret <= -1);
    
    //check timestamp diff
    ret = kvComparator.compare(put3, put4);
    if(PRINT){
      System.out.println("ret " +ret);
    }
    assertTrue(ret >= 1);

    //check type diff
    ret = kvComparator.compare(put1, del1);
    if(PRINT){
      System.out.println("ret " +ret);
    }
    assertTrue(ret >= 1);
  }
  
  public void testAdd(){
    memcache.newAdd(put1, multiFamily);
    
    KeyValue kv = memcache.memcache.first();
    int ret = kvComparator.compare(put1, kv);
    assertEquals(0, ret);
  }

  public void testMultiAdd(){
    memcache.newAdd(put1, multiFamily);
    assertEquals(1, memcache.memcache.size());

    memcache.newAdd(put1, multiFamily);
    assertEquals(1, memcache.memcache.size());
    
  
    memcache.newAdd(put2, multiFamily);
    assertEquals(2, memcache.memcache.size());

    memcache.newAdd(put3, multiFamily);
    assertEquals(3, memcache.memcache.size());
    
    memcache.newAdd(put4, multiFamily);
    assertEquals(4, memcache.memcache.size());

    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }

  }
  
  
  
  public void testAddAndDelete(){
    //Adding put
    memcache.newAdd(put1, multiFamily);

    //Adding delete for the same ts
    memcache.newAdd(del1, multiFamily);

    
    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }
    assertEquals(0, memcache.memcache.size());
  }  
  
  public void testAddAndDeleteColumn(){
    //Adding put
    memcache.newAdd(put1, multiFamily);

    //Adding deletecolumn
    memcache.newAdd(delCol1, multiFamily);
    assertEquals(1, memcache.memcache.size()); 

    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }
  }

  public void testAddAndDeleteFamily(){
    //Adding put
    memcache.newAdd(put1, multiFamily);

    //Adding deleteFamily
    memcache.newAdd(delFam1, multiFamily);

    assertEquals(1, memcache.memcache.size()); 

    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }
  }

  public void testAddAndMultiDelete(){
    //Adding put
    memcache.newAdd(put1, multiFamily);
    assertEquals(1, memcache.memcache.size()); 

    //Adding delete for the same ts
    memcache.newAdd(del1, multiFamily);
    assertEquals(0, memcache.memcache.size()); 
    
    //Adding delete for the same ts
    memcache.newAdd(delCol1, multiFamily);
    assertEquals(1, memcache.memcache.size()); 
    
    //Adding delete for the same ts
    memcache.newAdd(delFam1, multiFamily);
    assertEquals(1, memcache.memcache.size());
    
    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }
  }

  
  public void testAddAddAddMultiDeleteAdd(){
    //Adding put with a different row
    memcache.newAdd(put3, multiFamily);
    assertEquals(1, memcache.memcache.size());
    
    //Adding put with a different family
    memcache.newAdd(put2, multiFamily);
    assertEquals(2, memcache.memcache.size()); 

    //Adding put 
    memcache.newAdd(put1, multiFamily);
    assertEquals(3, memcache.memcache.size()); 
    
    printMemCache();
    
    //Adding delete for the same ts
    memcache.newAdd(del1, multiFamily);
    assertEquals(2, memcache.memcache.size()); 
    
    //Adding delete for the same ts
    memcache.newAdd(delCol1, multiFamily);
    assertEquals(3, memcache.memcache.size()); 
    
    //Adding delete for the same ts
    memcache.newAdd(delFam1, multiFamily);
    assertEquals(2, memcache.memcache.size());
    
    if(PRINT){
      System.out.println(new Exception().getStackTrace()[0].getMethodName());
      printMemCache();
      System.out.println();
    }
  }
  
  private void printMemCache(){
    for(KeyValue keyvalue : memcache.memcache){
      System.out.println("keyValue " +keyvalue);
    }
  }
}
