package org.apache.hadoop.hbase.io;

import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestFamily extends TestCase {
  
  public void testFamily(){
    byte [] fam = "fam1".getBytes();
    byte [][] cols = new byte[][]{new byte[]{12}, new byte[]{5}, new byte[]{35}}; 
    
    Family family = new Family(fam, cols[0]);
    
    for(byte [] col : cols){
      family.add(col);
      System.out.println("col " + toString(col));
    }

    System.out.println("");

    List<byte[]> li = family.getColumns();
    for(int i=0; i< li.size(); i++){
      System.out.println("col " + toString(li.get(i)));
    }
    
    //Checking if input is the same as output
    boolean same = false;
    for(int i=0; i< li.size(); i++){
      same = same || (li.get(i) != cols[i]); 
    }
    System.out.println("same " +same);
    assertTrue(same);
    
    //Check if list is sorted
    for(int i=0; i<li.size()-1; i++){
      assertTrue(Bytes.compareTo(li.get(i), li.get(i+1)) <= -1);
    }
    
  }

  private String toString(byte[] bytes) {
    String ret = "";
    for(int i=0;i<bytes.length;i++) {
      ret += String.format("%x ",bytes[i]);
    }
    return ret;
  }
}
