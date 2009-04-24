package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetColumns;
import org.apache.hadoop.hbase.io.GetFamilies;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import junit.framework.TestCase;

public class TestNewGet2 extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestNewGet2.class);
  private MiniDFSCluster cluster = null;
  private HRegion region = null;

  
  private static final byte [] CONTENTS = Bytes.toBytes("contents:");
  private static final byte [] ROW_KEY =
    HRegionInfo.ROOT_REGIONINFO.getRegionName();
  private static final String SERVER_ADDRESS = "foo.bar.com:1234";
  
  
  @Override
  public void setUp() throws Exception {
    try {
      this.cluster = new MiniDFSCluster(this.conf, 2, true, (String[])null);
      // Set the hbase.rootdir to be the home directory in mini dfs.
      this.conf.set(HConstants.HBASE_DIR,
        this.cluster.getFileSystem().getHomeDirectory().toString());
    } catch (IOException e) {
      shutdownDfs(cluster);
    }
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    shutdownDfs(cluster);
    // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
    //  "Temporary end-of-test thread dump debugging HADOOP-2040: " + getName());
  }
  
  public void testGet() throws IOException {
    try {
      
      HTableDescriptor desc = new HTableDescriptor("test");
      desc.addFamily(new HColumnDescriptor(CONTENTS));
      desc.addFamily(new HColumnDescriptor(HConstants.COLUMN_FAMILY));
      
      region = createNewHRegion(desc, null, null);
      HRegionIncommon r = new HRegionIncommon(region);
      
      // Write information to the table
      BatchUpdate batchUpdate = null;
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
      batchUpdate.put(CONTENTS, CONTENTS);
      batchUpdate.put(HConstants.COL_REGIONINFO, 
          Writables.getBytes(HRegionInfo.ROOT_REGIONINFO));
      r.commit(batchUpdate);
      
      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
      batchUpdate.put(HConstants.COL_SERVER, 
        Bytes.toBytes(new HServerAddress(SERVER_ADDRESS).toString()));
      batchUpdate.put(HConstants.COL_STARTCODE, Bytes.toBytes(12345));
      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) +
        "region", Bytes.toBytes("region"));
      r.commit(batchUpdate);
      
      // Verify that get works the same from memcache as when reading from disk
      // NOTE dumpRegion won't work here because it only reads from disk.
      
      verifyNewGet(r, SERVER_ADDRESS);

//      verifyGet(r, SERVER_ADDRESS);
//      
//      // Close and re-open region, forcing updates to disk
//      
//      region.close();
//      region = openClosedRegion(region);
//      r = new HRegionIncommon(region);
//      
//      // Read it back
//      
//      verifyGet(r, SERVER_ADDRESS);
//      
//      // Update one family member and add a new one
//      
//      batchUpdate = new BatchUpdate(ROW_KEY, System.currentTimeMillis());
//      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) + "region",
//        "region2".getBytes(HConstants.UTF8_ENCODING));
//      String otherServerName = "bar.foo.com:4321";
//      batchUpdate.put(HConstants.COL_SERVER, 
//        Bytes.toBytes(new HServerAddress(otherServerName).toString()));
//      batchUpdate.put(Bytes.toString(HConstants.COLUMN_FAMILY) + "junk",
//        "junk".getBytes(HConstants.UTF8_ENCODING));
//      r.commit(batchUpdate);
//
//      verifyGet(r, otherServerName);
//      
//      // Close region and re-open it
//      
//      region.close();
//      region = openClosedRegion(region);
//      r = new HRegionIncommon(region);
//
//      // Read it back
//      
//      verifyGet(r, otherServerName);

    } finally {
      if (region != null) {
        // Close region once and for all
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }
  
  private void verifyGet(final HRegionIncommon r, final String expectedServer)
  throws IOException {
    // This should return a value because there is only one family member
    Cell value = r.get(ROW_KEY, CONTENTS);
    assertNotNull(value);
    
    // This should not return a value because there are multiple family members
    value = r.get(ROW_KEY, HConstants.COLUMN_FAMILY);
    assertNull(value);
    
    // Find out what getFull returns
    Map<byte [], Cell> values = r.getFull(ROW_KEY);
    
    // assertEquals(4, values.keySet().size());
    for (Map.Entry<byte[], Cell> entry : values.entrySet()) {
      byte[] column = entry.getKey();
      Cell cell = entry.getValue();
      if (Bytes.equals(column, HConstants.COL_SERVER)) {
        String server = Writables.cellToString(cell);
        assertEquals(expectedServer, server);
        LOG.info(server);
      }
    }
  }
  
  private void verifyNewGet(final HRegionIncommon r, final String expectedServer)
  throws IOException {
    // This should return a value because there is only one family member
//    Get get = new GetColumns(rows[i], family, column, (byte)1, null);
    Get get = new GetFamilies(ROW_KEY, CONTENTS, (byte)1);
    List<KeyValue> result = new ArrayList<KeyValue>();
    Integer lock = null;
    r.newget(get, result, lock);
    System.out.println("Result");
    for(KeyValue kv : result){
      System.out.println("kv " +kv);
    }

//    Cell value = r.get(ROW_KEY, CONTENTS);
//    assertNotNull(value);
//    
//    // This should not return a value because there are multiple family members
//    value = r.get(ROW_KEY, HConstants.COLUMN_FAMILY);
//    assertNull(value);
//    
//    // Find out what getFull returns
//    Map<byte [], Cell> values = r.getFull(ROW_KEY);
//    
//    // assertEquals(4, values.keySet().size());
//    for (Map.Entry<byte[], Cell> entry : values.entrySet()) {
//      byte[] column = entry.getKey();
//      Cell cell = entry.getValue();
//      if (Bytes.equals(column, HConstants.COL_SERVER)) {
//        String server = Writables.cellToString(cell);
//        assertEquals(expectedServer, server);
//        LOG.info(server);
//      }
//    }
  }
  
}
