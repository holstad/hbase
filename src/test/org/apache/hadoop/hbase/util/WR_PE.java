package org.apache.hadoop.hbase.util;


/* Java */
import java.io.*;
import java.util.*;
//import java.util.regex.*;

/* Hadoop */
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/* HBase */
import org.apache.hadoop.hbase.*;
//import org.apache.hadoops.hbase.mapred.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.util.*;


/* Extra */
//import org.apache.commons.cli.ParseException;
//import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
//import org.apache.commons.httpclient.*;
//import org.apache.commons.httpclient.methods.*;
//import org.apache.commons.httpclient.params.HttpMethodParams;

public class WR_PE{

  private static byte [] tableName = "test".getBytes();
  private static HTable table = null;
  private static byte [] family = "fam:".getBytes();
  private static byte [] column = "col1".getBytes();
  private static byte [] familyAndColumn = Bytes.add(family, column);
  private static byte [][] families = {familyAndColumn};
  
  public static void main(String[] args)
  throws IOException{
    new WR_PE();
  }

  public WR_PE()
  throws IOException{

    final int LENGTH = 10000;
    final int NR_COLS = 1;
//    createTable();
    init();

//  sequentialWrites(LENGTH);
//  sequentialReads(LENGTH);
//  randomWrites(LENGTH);
//  randomWrites(LENGTH, NR_COLS);
//    randomReads(LENGTH);
    newRandomReads(LENGTH);
//  randomReads(1);
//  randomReadsMem();
//  scans();
  }

  public static void createTable()
  throws IOException{
    HBaseAdmin hba = new HBaseAdmin(new HBaseConfiguration());

    if(hba.tableExists(tableName)){
      hba.disableTable(tableName);
      hba.deleteTable(tableName);
    } 
    
    HColumnDescriptor hcd = new HColumnDescriptor(family);

    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(hcd);
    hba.createTable(htd);
    hba.enableTable(tableName);

  }

  public static void init()
  throws IOException{
    table = new HTable(tableName);
  }

  public static void randomWrites(final int LENGTH)
  throws IOException{
    byte[][] rows = randomArrayBuilder(LENGTH);
//  printRows(rows);

    final int SIZE = 1000; //in bytes
    byte[][] values = randomArrayBuilder(LENGTH, SIZE);
//  printValues(values);

    List<BatchUpdate> bus = new ArrayList<BatchUpdate>(LENGTH);
    BatchUpdate bu = null;

    long start = System.currentTimeMillis();
    for(int i=0; i<LENGTH; i++){
      bu = new BatchUpdate(rows[i]);
      bu.put(familyAndColumn, values[i]);
      bus.add(bu);
    }
    table.commit(bus);
    long stop = System.currentTimeMillis();
    long timer = stop - start;
    System.out.println("Random writes: Timer for " +LENGTH+ " / " +timer+
        " ms" + " = "+ (LENGTH/((float)timer/1000)));
  }



//  public static void randomWrites(final int LENGTH, final int NR_COLS)
//  throws IOException{
//    byte[][] rows = randomArrayBuilder(LENGTH);
////  printRows(rows);
//
//    final int SIZE = 1000; //in bytes
//    byte[][] values = randomArrayBuilder(LENGTH, SIZE);
////  printValues(values);
//
//    List<BatchUpdate> bus = new ArrayList<BatchUpdate>(LENGTH);
//    BatchUpdate bu = null;
//
//    byte[][] cols = addFamily(family, sequentialArrayBuilder(NR_COLS));
//
//    long start = System.currentTimeMillis();
//    for(int i=0; i<LENGTH; i++){
//      bu = new BatchUpdate(rows[i]);
//      for(int j=0; j<NR_COLS; j++){
//        bu.put(cols[j], values[i]);
//      }
//      bus.add(bu);
//      if(i*NR_COLS > 10*LENGTH){
//        table.commit(bus);
//        bus.clear();
//      }
//    }
//    table.commit(bus);
//    long stop = System.currentTimeMillis();
//    System.out.println("Random writes: Timer for " +LENGTH+ " and cols "
//        +NR_COLS+ " = " +(stop-start)+ " ms");
//  }


//  serverName 192.168.249.68:60020, region test,,1239386120833, row 6386

  public static void randomReads(final int LENGTH)
  throws IOException{
    byte[][] rows = randomArrayBuilder(LENGTH);

    RowResult res = null;

    long start = System.currentTimeMillis();
    for(int i=0; i<LENGTH; i++){
      res = table.getRow(rows[i], families);
//    table.getRow(rows[i]);
    }
    long stop = System.currentTimeMillis();
    System.out.println(new Date(start));
    System.out.println("start " +start+ ", stop " +stop);
    long timer = stop - start;
    System.out.println("Number of fam/col per row " +res.size());
    System.out.println("Random reads: Timer for " +LENGTH+ " / " +timer+
        " ms" + " = "+ (LENGTH/((float)timer/1000)));
//  System.out.println(res +", size "+res.size());
  }

  
//  serverName 192.168.249.68:60020, region test,,1239386120833, row 3784
//  HBaseClient$Connection [line: 478] - sendParam(Call)
//  HBaseClient [line: 712] - call(Writable, InetSocketAddress, UserGroupInformation)
  public static void newRandomReads(final int LENGTH)
  throws IOException{
    byte[][] rows = randomArrayBuilder(LENGTH);
    
    List<Get> gets = randomGetColumnsListBuilder(LENGTH);

//    List<KeyValue> res = null;
    KeyValue [] res = null;


    Iterator<Get> getIter = gets.iterator();
    long start = System.currentTimeMillis();
    while(getIter.hasNext()){
      res = table.get(getIter.next());
    }
    long stop = System.currentTimeMillis();
    System.out.println(new Date(start));
    System.out.println("start " +start+ ", stop " +stop);
    long timer = stop - start;
//    System.out.println("Number of fam/col per row " +res.size());
    System.out.println("Number of fam/col per row " +res.length);

    System.out.println("Random reads: Timer for " +LENGTH+ " / " +timer+
        " ms" + " = "+ (LENGTH/((float)timer/1000)));
//  System.out.println(res +", size "+res.size());
  }
  
  
//  public static void sequentialWrites(final int LENGTH)
//  throws IOException{
//    byte[][] rows = sequentialArrayBuilder(LENGTH);
////  printRows(rows);
//
//    final int SIZE = 1000; //in bytes
//    byte[][] values = randomArrayBuilder(LENGTH, SIZE);
////  printValues(values);
//
//    List<BatchUpdate> bus = new ArrayList<BatchUpdate>(LENGTH);
//    BatchUpdate bu = null;
//
//    long start = System.currentTimeMillis();
//    for(int i=0; i<LENGTH; i++){
//      bu = new BatchUpdate(rows[i]);
//      bu.put(family, values[i]);
//      bus.add(new BatchUpdate(bu));
//    }
//    table.commit(bus);
//    long stop = System.currentTimeMillis();
//    System.out.println("Sequential writes: Timer for " +LENGTH+ " = " +(stop-start)+ " ms");
//  }
//
//  public static void sequentialReads(final int LENGTH)
//  throws IOException{
//    byte[][] rows = sequentialArrayBuilder(LENGTH);
//
//    RowResult res = null;
//
//    long start = System.currentTimeMillis();
//    for(int i=1; i<LENGTH; i++){
//      table.getRow(rows[i]);
//    }
//    long stop = System.currentTimeMillis();
//    System.out.println("Sequential writes: Timer for " +LENGTH+ " = " +
//        (stop-start)+ " ms");
//  }
//
//  public static void scans()
//  throws IOException{
//    RowResult res = null;
//    int i=0;
//    long start = System.currentTimeMillis();
//    byte[][] cols = {family};
//    Scanner scan = table.getScanner(cols);
//    while((res=scan.next()) != null){
//      i++;
//    }
//    long stop = System.currentTimeMillis();
//    System.out.println("Scans: Timer for " +i+ " = " +(stop-start)+ " ms");
//  }



//Helpers
  public static byte[][] randomArrayBuilder(final int LENGTH){
    Set<Integer> set = new HashSet<Integer>();
    Random rand = new Random();
    byte[][] ret = new byte[LENGTH][];
    for(int i=0; i<LENGTH; i++){
      Integer pos = 0;
      while(set.contains(pos = new Integer(rand.nextInt(LENGTH)))){}

      set.add(pos);
      ret[i] = toBytes(pos.intValue()); 
    }
    return ret;
  }
  public static byte[][] randomArrayBuilder(final int LENGTH, final int SIZE){
    Random rand = new Random();
    byte[][] ret = new byte[LENGTH][];
    byte[] bs = null;
    for(int i=0; i<LENGTH; i++){
      bs = new byte[SIZE];
      rand.nextBytes(bs);
      ret[i] = bs; 
    }
    return ret;
  }

  public static List<Get> randomGetRowListBuilder(final int LENGTH){
    List<Get> gets = new ArrayList<Get>();
    byte[][] rows = randomArrayBuilder(LENGTH);
    for(int i=0; i<LENGTH; i++){
      gets.add(new GetRow(rows[i], (byte)1, null));
    }
    
    return gets;
  }

  public static List<Get> randomGetColumnsListBuilder(final int LENGTH){
    List<Get> gets = new ArrayList<Get>();
    byte[][] rows = randomArrayBuilder(LENGTH);
    for(int i=0; i<LENGTH; i++){
      gets.add(new GetColumns(rows[i], family, column, (byte)1, null));
    }
    
    return gets;
  }
  
  
  public static byte[][] sequentialArrayBuilder(final int LENGTH){
    byte[][] ret = new byte[LENGTH][];
    for(int i=0; i<LENGTH; i++){
      ret[i] = toBytes(i); 
    }
    return ret;
  }


  public static void printRows(byte[][] arr){
    for(int i=0; i<arr.length; i++){
      System.out.print(toInt(arr[i]) +"\t");
    }
    System.out.println();
  }
  public static void printValues(byte[][] arr){
    String hex = "";
    for(int i=0; i<arr.length; i++){
      for(int j=0; j<arr[i].length; j++){
        hex = String.format("%x ", arr[i][j]);
        System.out.print(hex);
      }
      System.out.print("\t");
    }
    System.out.println();
  }


  public static int toInt(byte[] bytes) {
    if (bytes.length > 4)
      throw new IllegalArgumentException("Must specify 4 bytes or less");

    int returnInt = 0;
    for (int n = 0; n < bytes.length; n++) {
      returnInt <<= 8;
      int aByte = bytes[n] < 0 ? bytes[n] + 256 : bytes[n];
      returnInt = returnInt | aByte;
    }
    return returnInt;
  }
  public static byte[] toBytes(int n) {
    byte [] b = new byte[4];
    for(int i=3;i>0;i--) {
      b[i] = (byte)(n);
      n >>>= 8;
    }
    b[0] = (byte)(n);
    return b;
  }

  public static byte[][] addFamily(byte[] fam, byte[][] cols){
    int size = cols.length;
    byte[][] res = new byte[size][];
    byte[] col = null;
    for(int i=0; i<cols.length; i++){
      col = new byte[fam.length+cols[i].length];
      System.arraycopy(fam, 0, col, 0, fam.length);
      System.arraycopy(cols[i], 0, col, fam.length, cols[i].length);
      res[i] = col;
//    System.out.println(new String(res[i]));
    }
    return res;
  }
}
