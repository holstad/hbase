package org.apache.hadoop.hbase.io;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

public class TestTimeRange extends TestCase {
  final boolean PRINT = true;
  long tsMin = 0L;
  long tsCur = 0L;
  long tsMax = 0L;
  byte [] tsCurBytes = null;
  
  
  protected void setUp() throws Exception {
    super.setUp();
  }
  
  public void testNewer()
  throws IOException{
    tsMin = System.nanoTime();
    tsMax = System.nanoTime();
    tsCur = System.nanoTime();
    tsCurBytes = Bytes.toBytes(tsCur);
    TimeRange tr = new TimeRange(tsMax, tsMin);
    int ret = tr.withinTimeRange(tsCurBytes, 0);
    if(PRINT) System.out.println("ret " +ret);
    assertEquals(0, ret);
  }

  public void testOlder()
  throws IOException{
    tsCur = System.nanoTime();
    tsCurBytes = Bytes.toBytes(tsCur);
    tsMin = System.nanoTime();
    tsMax = System.nanoTime();
    TimeRange tr = new TimeRange(tsMax, tsMin);
    int ret = tr.withinTimeRange(tsCurBytes, 0);
    if(PRINT) System.out.println("ret " +ret);
    assertEquals(0, ret);
  }
  
  public void testTsInbetween()
  throws IOException{
    //Size inbetween
    tsMin = System.nanoTime();
    tsCur = System.nanoTime();
    tsCurBytes = Bytes.toBytes(tsCur);
    tsMax = System.nanoTime();
    TimeRange tr = new TimeRange(tsMax, tsMin);
    int ret = tr.withinTimeRange(tsCurBytes, 0);
    if(PRINT) System.out.println("ret " +ret);
    assertEquals(1, ret);    
  }

  public void testTsSameAsMin()
  throws IOException{
    //Size inbetween
    tsMin = System.nanoTime();
    tsCur = tsMin;
    tsCurBytes = Bytes.toBytes(tsCur);
    tsMax = System.nanoTime();
    TimeRange tr = new TimeRange(tsMax, tsMin);
    int ret = tr.withinTimeRange(tsCurBytes, 0);
    if(PRINT) System.out.println("ret " +ret);
    assertEquals(1, ret);    
  }

  public void testTsSameAsMax()
  throws IOException{
    //Size inbetween
    tsMin = System.nanoTime();
    tsMax = System.nanoTime();
    tsCur = tsMax;
    tsCurBytes = Bytes.toBytes(tsCur);
    TimeRange tr = new TimeRange(tsMax, tsMin);
    int ret = tr.withinTimeRange(tsCurBytes, 0);
    if(PRINT) System.out.println("ret " +ret);
    assertEquals(0, ret);    
  }
}
