package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.GetTop;
import org.apache.hadoop.hbase.util.Bytes;

public class ServerGetTop extends AbstractServerGet {
//  private static final Log LOG = LogFactory.getLog(ServerGetTop.class);

  private int counter = 0;

  public ServerGetTop(Get get){
    super(get);
    this.counter = ((GetTop)get).getFetches();
  }

  @Override
  public int compareTo(KeyValue kv, boolean multiFamily) {
    if(isDone()){
      return super.DONE;
    }

    int initialOffset = kv.getOffset();
    int offset = initialOffset;
    byte [] bytes = kv.getBuffer();

    //Getting key length
    int keyLen = Bytes.toInt(bytes, offset);
    offset += Bytes.SIZEOF_INT;

    //Skipping valueLength
    offset += Bytes.SIZEOF_INT;

    //Getting row length
    short rowLen = Bytes.toShort(bytes, offset);
    offset += Bytes.SIZEOF_SHORT;

    byte [] row = super.getRow();
    int ret = Bytes.compareTo(row, 0, row.length, bytes, offset, rowLen);
    if(ret <= -1){
      return super.NEXT_SF;
    } else if(ret >= 1){
      return super.NEXT_KV; 
    }
    offset += rowLen;
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("t " + System.nanoTime());
//    }
    //This is only for the future if we have more than on family in the same
    //storefile, can be turned off for now
    //Getting family length
    byte famLen = bytes[offset];
    offset += Bytes.SIZEOF_BYTE;

    if(multiFamily){
      byte [] family = super.getFamily();
      ret = Bytes.compareTo(family, 0, family.length, bytes, offset, rowLen);
      if(ret <= -1){
        return super.NEXT_SF;
      } else if(ret >= 1){
        return super.NEXT_KV; 
      }
    }
    offset += famLen;

    //Getting column length
    int colLen = super.KEY_OFFSET + keyLen - (offset-initialOffset) - 
    Bytes.SIZEOF_LONG - Bytes.SIZEOF_BYTE;
    //Could be switched to something like:
    //initloffset + keylength - TIMESTAMP_TYPE_SIZE - loffset;

    //Checking TTL and TimeRange before column so that if the entry turns out
    //to be a delete we can be sure that all the deletes in the delete list
    //are valid deletes
    int tsOffset = offset + colLen;
    long ts = Bytes.toLong(bytes, tsOffset);
    ret = super.checkTTL(ts);
    if(ret == 0){
      return super.NEXT_KV;
    }

    ret = super.get.getTimeRange().withinTimeRange(ts);
    if(ret != 1){
      return super.NEXT_KV;
    }
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("t " + System.nanoTime());
//    }
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("t " + System.nanoTime());
//    }
    //Check if kv is a delete
    if(super.isDelete(bytes, initialOffset, keyLen)){
      super.newDeletes.add(kv);
      return super.NEXT_KV;
    }     

    //TODO
    //ret = compareFilter(kv);

    //Check if this kv is deleted
    if(!super.deletes.isEmpty()){
      ret = super.isDeleted(bytes, initialOffset, rowLen, famLen,
          colLen, multiFamily);
      if(ret != 0){
        return super.NEXT_KV;
      }
    }

    //Includes remove from getList, TODO change for other calls
    counter--;
    return super.ADD;
  }

  private boolean isDone(){
    if(counter <= 0){
      return true;
    }
    return false;
  }

}
