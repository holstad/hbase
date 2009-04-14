package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;

public class TestSingleLinkedList extends TestCase {
  int size = 10;
  List<Integer> compList = new LinkedList<Integer>();
  List<Integer> list = new SingleLinkedList<Integer>();
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    addToList(compList);
    addToList(list);
  }
  
  public void testWrite(){
    assertEquals(size, list.size());
    Iterator<Integer> iter = list.iterator();
    Iterator<Integer> compIter = compList.iterator();
    while(iter.hasNext()){
      assertEquals(iter.next(), compIter.next());
    }
  }

  public void testRemove(){
    Iterator<Integer> iter = list.iterator();
    Integer curr = 0;

    Integer rem0 = 0;
    Integer rem5 = 5;
    Integer rem9 = 9;
    while(iter.hasNext()){
      curr = iter.next();
      if(curr == rem0){
        iter.remove();
      } else if(curr == rem5){
        iter.remove();
      } else if(curr == rem9){
        iter.remove();
      }
    }
    iter = list.iterator();
    while(iter.hasNext()){
      curr = iter.next();
      assertFalse(curr == rem0);
      assertFalse(curr == rem5);
      assertFalse(curr == rem9);
    }
  }
  
  //The timing test shows that arrayList is the fastest since it allocates
  //big blocks at a time. But the SingleLinkedList is faster than the Double
  //and the memory footprint is smaller 
  public void testAddTiming(){
    List<Integer> list = null;
    size = 100000;
    long start = 0L;
    long stop = 0L;
    
    //ArrayList
    System.out.println("ArrayList");
    start = System.nanoTime();
    list = new ArrayList<Integer>();
    addToList(list);
    stop = System.nanoTime();
    System.out.println("timer " +(stop - start));

//    //SingleLinkedList
//    System.out.println("SingleLinkedList");
//    start = System.nanoTime();
//    list = new SingleLinkedList<Integer>();
//    addToList(list);
//    stop = System.nanoTime();
//    System.out.println("timer " +(stop - start));

//    //LinkedList
//    System.out.println("LinkedList");
//    start = System.nanoTime();
//    list = new LinkedList<Integer>();
//    addToList(list);
//    stop = System.nanoTime();
//    System.out.println("timer " +(stop - start));
    
  }
  
  //Helpers
  private void addToList(List list){
    for(int i=0; i<size; i++){
      list.add(i);
    }
  }
  private void printList(List l){
    Iterator iter = l.iterator();
    while(iter.hasNext()){
      System.out.print(iter.next() +" ");
    }
    System.out.println();
  }
}
