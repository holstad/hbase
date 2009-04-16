package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.filter.RowFilterInterface;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.Get;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractServerGet implements ServerGet{
//  byte[] row = null;
//  Family family = null;
//  TimeRange tr = null;
  protected Get get = null;
  private byte [] family = null;
  
  private Iterator<Key> deleteIterator = null;
  private Key delete = null;
  private byte [] deleteFamilyBytes = null;


  protected Iterator<byte[]> columnIterator = null;
  protected byte [] column = null;
  
  
  private static final int KEY_OFFSET = 2*Bytes.SIZEOF_INT;
  private static final int KEY_SIZES = Bytes.SIZEOF_SHORT + Bytes.SIZEOF_BYTE +
  Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE;
  
  //This is the list of columnNames + one more byte for the count.
  //For GetFamilies and GetTop values are copied into this list. My tests show
  //that copying an array is faster than creating a new object, for example
  //a Tuple<KeyValue, Integer>.
  
  //Created a new class, SingleLinkedList to compare with the LinkedList and the
  //ArrayList and it turns out the the ArrayList is faster for puts and more
  //memory efficient than the other 2. Remove is slower since the whole array
  //needs to be copied, but since this is only happening in one place,
  //ServerGetColumns.updateVersions and probably is not going to happen to often
  //leaving it as it is.
  //Another interesting thing is when using Arrays.asList it casts your list to
  //an AbstractList so you can't use remove and it is also like 10x slower than
  //doing it yourself in a for loop.
  //So using ArrayLists for everything for now.
  List<byte[]> columns = null;

  
  //Same thing as above goes for this
  List<Key> deletes = new ArrayList<Key>();

  RowFilterInterface filter = null;
  
  long ttl = 0L;
  long now = 0L;
  
  public AbstractServerGet(Get get){
    this.get = get;
    this.columns = new ArrayList<byte []>();
    this.deletes = new ArrayList<Key>();
  }
  
  public void clear(){
    this.column = null;
    this.columnIterator = null;
    
    this.delete = null;
    this.deleteIterator = null;
  }
  
  public byte [] getFamily(){
    return family;
  }
  public void setFamily(byte [] family){
    this.family = family;
  }
  
  
  public byte [] getRow(){
    return get.getRow();
  }
  
  public TimeRange getTimeRange(){
    return get.getTimeRange();
  }


  
  public boolean isEmpty(){
    return (columns.size() == 0);
  }

  @Override
  public List<byte[]> getColumns(){
    return columns; 
  }
//  @Override
//  public void setColumns(List<byte[]> columns){
//    this.columns = columns; 
//  }
  @Override
  public void setColumns(byte [][] columns){
    for(int i=0; i<columns.length; i++){
      this.columns.add(columns[i]);
    }
  }  
  public List<Key> getDeletes(){
    return deletes; 
  }
  
  @Override
  public void setDeletes(List<Key> deletes) {
    this.deletes = deletes;
  }
 
  @Override
  public void setFilter(RowFilterInterface filter){
    this.filter = filter; 
  }

  @Override
  public long getNow(){
    return now;
  }
  
  @Override
  public void setNow(){
    this.now = System.currentTimeMillis();
  }

  @Override
  public long getTTL(){
    return ttl;
  }
  @Override
  public void setTTL(long ttl){
    this.ttl = ttl;
  }
  
  @Override
  public int compareTo(KeyValue kv)
  throws IOException{
    return compareTo(kv, false);
  }
  
//  public byte getVersion(){
//    byte [] currentColumn = columns.getCurrent();
//    return currentColumn[currentColumn.length-1];
//  }
//  
//  public void setVersion(byte version){
//    byte [] currentColumn = columns.getCurrent();
//    currentColumn[currentColumn.length-1] = version;
//  }
  
  public int getMaxVersions(){
    return get.getVersions();
  }
  

  protected int checkTTL(byte [] bytes, int offset){
    //Check if KeyValue is still alive
    if((this.now - this.ttl) > Bytes.toLong(bytes, offset)){
      return 1;
    }
    return 0; 
  }
  protected int checkTTL(long ts){
    //Check if KeyValue is still alive
    if((this.now - this.ttl) > ts){
      return 1;
    }
    return 0; 
  }
  
  protected boolean isDelete(byte [] bytes, int initialOffset, int keyLen){
    byte type = bytes[initialOffset + KEY_OFFSET + keyLen -1];
    return (type != KeyValue.Type.Put.getCode());
  }
 
  /**
   * This method are very different than the other compares, because it will
   * loop through the columns in the column list until the current kv is found
   * or the column is smaller than the kv.
   */
  protected int compareColumn(byte [] bytes, int offset, int colLen){
    if(columnIterator == null){
      columnIterator = columns.iterator();
      if(columnIterator.hasNext()){
        column = columnIterator.next();
      }
    }

    int res = 0;
    while(true){
      res = Bytes.compareTo(column, 0, column.length-1, bytes, offset, colLen);
      if(res >= 0){
        return res;
      }
      if(columnIterator.hasNext()){
        column = columnIterator.next();
      } else {
        return res;
      }
    }
  }
  
  protected int isDeleted(byte [] currBytes, int initCurrOffset,
    short currRowLen, byte currFamLen, int currColLen, boolean multiFamily){
    
    //Check if this is the first time isDeleted is called
    if(deleteIterator == null){
      deleteIterator = deletes.iterator();
      delete = deleteIterator.next();
    } else if(!deleteIterator.hasNext()){
      return 0;
    }
   
    int ret = 0;
    int currOffset = 0;

    byte [] delBytes = null;
    int delOffset = 0;
    int delKeyLen = 0;
    short delRowLen = 0;
    int delColLen = 0;
    int tsLen = Bytes.SIZEOF_LONG;
    
    while(true){
      //Clearing the last offset
      currOffset = initCurrOffset;
      
      //Setting up delete KeyValue
      delBytes = delete.getBuffer();

      //Getting key length
      delKeyLen = Bytes.toInt(delBytes, delOffset);
      delOffset = Bytes.SIZEOF_INT;
      
      //Skipping value length 
      delOffset += Bytes.SIZEOF_INT;

      //Getting row length
      delRowLen = Bytes.toShort(delBytes, delOffset);
      delOffset += Bytes.SIZEOF_SHORT;
      
      //Skipping the key and value lengths + row length
      currOffset += 2*Bytes.SIZEOF_INT + Bytes.SIZEOF_SHORT;
      
      //Skipping compare row since it already has been checked
      currOffset += currRowLen;
      delOffset += delRowLen;
      
      //Getting family length
      byte delFamLen = delBytes[delOffset];
      delOffset += Bytes.SIZEOF_BYTE;
      currOffset += Bytes.SIZEOF_BYTE;

      //CompareFamily
      if(multiFamily){
        ret = Bytes.compareTo(currBytes, currOffset, currFamLen, delBytes,
            delOffset, delFamLen);
        if(ret <= -1){
          return -1;
        } else if(ret >= 1){
          if(deleteIterator.hasNext()){
            delete.set(deleteIterator.next());
            continue;
          }
          return 0;
        }
      }
      currOffset += currFamLen;
      delOffset += delFamLen;
      
      //CompareColumn
      delColLen = delKeyLen - Bytes.SIZEOF_SHORT - delRowLen -
        Bytes.SIZEOF_BYTE - delFamLen - Bytes.SIZEOF_LONG - Bytes.SIZEOF_BYTE;
      ret = Bytes.compareTo(currBytes, currOffset, currColLen, delBytes,
        delOffset, delColLen);
      if(ret <= -1){
        return -1;
      } else if(ret >= 1){
        if(deleteIterator.hasNext()){
          delete.set(deleteIterator.next());
          continue;
        }
        return 0;
      }
      currOffset += currColLen;
      delOffset += delColLen;
      
      //Compare DeleteFamily
      if(deleteFamilyBytes != null){
        ret = Bytes.compareTo(currBytes, currOffset, tsLen, deleteFamilyBytes,
          0, tsLen);
        if(ret >= 1){
          return 1;
        }
      }
      
      
      //Compare ts and type
      //compareTs
      ret = Bytes.compareTo(currBytes, currOffset, tsLen, delBytes, delOffset,
        tsLen);
      if(ret == 0){
        return -1;
      } else if(ret >= 1) {
        return 0;
      } else {
        //Getting type
        delOffset += Bytes.SIZEOF_LONG;
        byte type = delBytes[delOffset];
        if(type == KeyValue.Type.DeleteColumn.getCode()){
          return -1;
        }
        
        if(deleteIterator.hasNext()){
          delete.set(deleteIterator.next());
          continue;
        }
        return 0;
      }
    }
  }  
  

  @Override
  public Deletes mergeDeletes(List<Key> l1, List<Key> l2){
    return mergeDeletes(l1, l2, false);
  }
  
  
  //This method should not be here
  @Override
  public Deletes mergeDeletes(List<Key> l1, List<Key> l2, boolean multiFamily){
    //TODO Add check for deleteFamily
    long deleteFamily = 0L;
    
//    List<Key> mergedDeletes = new LinkedList<Key>();
    List<Key> mergedDeletes = new ArrayList<Key>();

    if(l1.isEmpty()){
      if(l2.isEmpty()){
        return null;
      }
    } else if(l2.isEmpty()){
      return new Deletes(l1, 0);
    }
    
    Iterator<Key> l1Iter = l1.iterator();
    Key k1 = null;
    Iterator<Key> l2Iter = l2.iterator();
    Key k2  = l2Iter.next();
    
    //Check for deleteFamily, only need to do on the second argument since the
    //first one is already checked
    byte [] k2Bytes = k2.getBuffer();
    int k2Offset = k2.getOffset();
    int k2KeyLen = Bytes.toInt(k2Bytes, k2Offset);
    int k2TypeOffset = k2Offset + KEY_OFFSET + k2KeyLen - Bytes.SIZEOF_BYTE;
    if(k2Bytes[k2TypeOffset] == KeyValue.Type.DeleteFamily.getCode()){
      deleteFamily = Bytes.toLong(k2Bytes, k2TypeOffset - Bytes.SIZEOF_LONG);
      if(l2Iter.hasNext()){
        k2 = l2Iter.next();
      } else {
        if(l1.isEmpty()){
          return new Deletes(l2, deleteFamily);
        }
        return new Deletes(l1, deleteFamily);
      }
    }
    
    int ret = 0;
//    boolean it2 = false;
    int i2break = 0;
    while(l1Iter.hasNext()){
      k1 = l1Iter.next();
      while(true){
        ret = compareDeleteKeys(k1, k2, multiFamily);
        if(ret == -3){
          if(l2Iter.hasNext()){
            k2 = l2Iter.next();
            continue;
          }
          i2break = 2;
          break;
        } else if(ret == -2){
          mergedDeletes.add(k1);
          if(l2Iter.hasNext()){
            k2 = l2Iter.next();
          } else {
            i2break = 1;
          }
          break;
        } else if(ret == -1){
          mergedDeletes.add(k1);
          break;
        } else if(ret == 0){
          mergedDeletes.add(k1);
          if(l2Iter.hasNext()){
            k2 = l2Iter.next();
          } else {
            i2break = 1;
          }
          break;
        } else if(ret == 1){
          mergedDeletes.add(k2);
          if(l2Iter.hasNext()){
            k2 = l2Iter.next();
            continue;
          } 
          i2break = 1;
          break;
        } else if(ret == 2){
          mergedDeletes.add(k2);
          if(l2Iter.hasNext()){
            k2 = l2Iter.next();
          } else {
            i2break = 1;
          }
          break;
        } else {
          break;
        }
      }
      if(i2break != 0){
        break;
      }
    }
    
    
    
    
    if(i2break != 0){
      if(i2break == 2){
        mergedDeletes.add(k1);
      }
      while(l1Iter.hasNext()){
        mergedDeletes.add(l1Iter.next());
      }
//    }   
    
    
//    if(it2){
//      mergedDeletes.add(k1);
//      while(l1Iter.hasNext()){
//        mergedDeletes.add(l1Iter.next());
//      }
    } else {
      mergedDeletes.add(k2);
      while(l2Iter.hasNext()) {
        mergedDeletes.add(l2Iter.next());
      } 
    }
    
    return new Deletes(mergedDeletes, deleteFamily);
  } 
  
  //Same as compareDelete, should fix that later
  /**
   * @param k1 the first Key to compare 
   * @param k2 the second Key to compare
   * 
   * @return -2 if k1 was a deleteColumn, -1 if k1 should be kept, 0 if same, 
   * 1 if k2 should be kept and 2 if k2 was a deleteColumn
   */
  private int compareDeleteKeys(Key k1, Key k2, boolean multiFamily){
    byte [] k1Bytes = k1.getBuffer();
    int k1Offset = k1.getOffset();

    byte [] k2Bytes = k2.getBuffer();
    int k2Offset = k2.getOffset();

    //Getting key lengths
    int k1KeyLen = Bytes.toInt(k1Bytes, k1Offset);
    k1Offset += Bytes.SIZEOF_INT;
    int k2KeyLen = Bytes.toInt(k2Bytes, k2Offset);
    k2Offset += Bytes.SIZEOF_INT;

    //Skipping value lengths
    k1Offset += Bytes.SIZEOF_INT;
    k2Offset += Bytes.SIZEOF_INT;

    //Getting row lengths
    short k1RowLen = Bytes.toShort(k1Bytes, k1Offset);
    k1Offset += Bytes.SIZEOF_SHORT;
    short k2RowLen = Bytes.toShort(k2Bytes, k2Offset);
    k2Offset += Bytes.SIZEOF_SHORT;

    //Skipping reading row, since it has already been checked
    k1Offset += k1RowLen;
    k2Offset += k2RowLen;

    int ret = 0;
    //Getting family lengths
    byte k1FamLen = k1Bytes[k1Offset];
    k1Offset += Bytes.SIZEOF_BYTE;
    byte k2FamLen = k2Bytes[k2Offset];
    k2Offset += Bytes.SIZEOF_BYTE;

    //Compare families
    if(multiFamily){
      ret = Bytes.compareTo(k1Bytes, k1Offset, k1FamLen, k2Bytes, k2Offset,
          k2FamLen);
      if(ret <= -1){
        return -1;
      } else if(ret >=1){
        return 1;
      }
    }
    k1Offset += k1FamLen;
    k2Offset += k2FamLen;

    //Get column lengths
    int k1ColLen = k1KeyLen - k1RowLen - k1FamLen - KEY_SIZES;
    int k2ColLen = k2KeyLen - k2RowLen - k2FamLen - KEY_SIZES;
    
    //Compare columns
    ret = Bytes.compareTo(k1Bytes, k1Offset, k1ColLen, k2Bytes, k2Offset,
        k2ColLen);
    if(ret <= -1){
      return -1;
    } else if(ret >= 1){
      return 1;
    }
    k1Offset += k1ColLen;
    k2Offset += k2ColLen;    

    //compare types
    byte k1type = k1Bytes[k1Offset + Bytes.SIZEOF_LONG];
    byte k2type = k2Bytes[k2Offset + Bytes.SIZEOF_LONG];
    ret = (0xff & k1type) - (0xff & k2type);
    
    if(ret == 0){
      ret = Bytes.compareTo(k2Bytes, k2Offset, Bytes.SIZEOF_LONG, k1Bytes,
        k1Offset, Bytes.SIZEOF_LONG);
      if(ret >= 1){
        ret = 2;
      } else if(ret <= -1){
        ret = -2;
      }
      return ret;
    } else if(ret <= -1){
      ret = Bytes.compareTo(k2Bytes, k2Offset, Bytes.SIZEOF_LONG, k1Bytes,
          k1Offset, Bytes.SIZEOF_LONG);
      if(ret <= -1){
        return 3;
      } else if(ret == 0){
        return 3;
      } else {
        return -1;
      }
    } else {
      ret = Bytes.compareTo(k2Bytes, k2Offset, Bytes.SIZEOF_LONG, k1Bytes,
          k1Offset, Bytes.SIZEOF_LONG); 
      if(ret <= -1){
        return -3;
      } else if(ret == 0){
        return -3;
      } else {
        return 1;
      }
    }
  }
  
  
  /**
   * For now only returns the family and the columns
   * Not the fastest implementation though columns is a LinkedList, but not too
   * concerned about that right now.
   * TODO speed it up 
   * 
   * @return a string representation of the object
   */
  public String toString(){
    StringBuffer sb = new StringBuffer();
    sb.append("/");
    sb.append(new String(this.family));
    sb.append(", columns[");
    int i=0;
    byte [] col = null;
    int len = 0;
    for(; i<columns.size()-1; i++){
      col = columns.get(i);
      len = col.length;
      sb.append(new String(col, 0, len-2));
      sb.append("-");
      //The number of versions fetched
      sb.append(col[len-1]);
      sb.append(", ");
    }
    if(columns != null && columns.size() > 0){
      col = columns.get(i);
      len = col.length;
      sb.append(new String(col, 0, col.length-2));
      sb.append("-");
      //The number of versions fetched
      sb.append(col[len-1]);
    }
    sb.append("]");    
    
   return sb.toString(); 
  }
  
}
