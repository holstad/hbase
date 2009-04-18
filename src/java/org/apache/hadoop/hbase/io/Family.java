package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;


public class Family implements Writable{
  protected byte [] family = new byte [0];
  protected List<byte[]> columns = new ArrayList<byte[]>();
  protected final byte[] ZERO_BYTES = new byte[]{(byte)0};
  
  public Family(){}
  public Family(byte[] family){
    this.family = family;
  }
  public Family(byte[] family, byte[] column) {
    this.family = family;
    this.columns.add(buildColumn(column, ZERO_BYTES));
  }
  public Family(byte[] family, List<byte[]> columns) {
    this.family = family;
    for(byte[] column : columns){
      this.columns.add(buildColumn(column, ZERO_BYTES));
    }
  }
  
  //Puts
  public Family(byte[] family, byte[] column, byte[] value) {
    this.family = family;
    this.columns.add(buildColumn(column, value));
  }
  public Family(byte[] family, Map<byte[], byte[]> map) {
    this.family = family;
    for(Map.Entry<byte[], byte[]> entry : map.entrySet()){
      this.columns.add(buildColumn(entry.getKey(), entry.getValue()));
    }
  }
  
  public void add(byte[] column){
    this.columns.add(buildColumn(column, ZERO_BYTES));
  }
  
  public void add(byte[] column, byte[] value){
    this.columns.add(buildColumn(column, value));
  }
  
  /**
   * 
   * @return the family
   */
  public byte [] getFamily() {
    return family;
  }
  /**
   * Sets the family name of this Family
   * @param family
   */
  public void setFamily(byte [] family){
    this.family = family;
  }
  
  public List<byte[]> getColumns() {
    return columns;
  }
  
  
  //TODO have to check how thos sorts
  public void sortColumns(){
    byte[][] cols = new byte[columns.size()][];
    cols = columns.toArray(new byte[0][]);
    Arrays.sort(cols, new FamilyComparator());
    columns.clear();
    for(byte[] bytes : cols){
      columns.add(bytes);
    }
  }
  
  public void createKeyValuesFromColumns(byte[] row, final long ts){
    List<byte[]> updatedColumns = new ArrayList<byte[]>(columns.size());
    byte[] col = null;
    int colLen = 0;
    byte[] val = null;
    int valOffset = 0;
    for(byte[] column : columns){
      colLen = Bytes.toInt(column);
      valOffset = Bytes.SIZEOF_INT + colLen; 
      updatedColumns.add(new KeyValue(row, this.family, column, Bytes.SIZEOF_INT, 
        colLen, ts, KeyValue.Type.Put,
        column, valOffset, column.length - valOffset).getBuffer());
    }
    columns = updatedColumns;
  }
  
  private byte[] buildColumn(byte[] column, byte[] value){
    return Bytes.add(Bytes.toBytes(column.length), column, value);
  }
  
  
  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();

    sb.append("Family ");
    sb.append(new String(family));
    sb.append(", columns [");
    int i = 0;
    byte [] column = null;
    int colLen = 0;
    int pos = 0;
    for(; i<columns.size()-1; i++){
      appendToBuffer(columns.get(i), sb);
//      column = columns.get(i);
//      sb.append("(column size ");
//      colLen = Bytes.toInt(column, pos, Bytes.SIZEOF_INT);
//      pos += Bytes.SIZEOF_INT;
//      sb.append(colLen);
//      sb.append(" ,column ");
//      sb.append(new String(column, pos, colLen));
//      pos += colLen;
//      sb.append(" ,value ");
//      sb.append(new String(column, pos, column.length -1 - pos));
      sb.append("), ");      
    }

//    sb.append(new String(columns.get(i)));//, 0, columns.get(i).length -1));
    appendToBuffer(columns.get(i), sb);
    sb.append("]"); 
    return sb.toString();
  }
  private void appendToBuffer(byte[] column, StringBuffer sb){
    sb.append("(column size ");
    int colLen = Bytes.toInt(column);
    int pos = Bytes.SIZEOF_INT;
    sb.append(colLen);
    sb.append(" ,column ");
    sb.append(new String(column, pos, colLen));
    pos += colLen;
    sb.append(" ,value ");
    sb.append(new String(column, pos, column.length - pos));
//    sb.append("), "); 
  }
  
  //Writable
  public void readFields(final DataInput in)
  throws IOException {
    this.family = Bytes.readByteArray(in);
    int nColumns = in.readInt();
    this.columns = new ArrayList<byte[]>(nColumns);
    for(int i=0; i<nColumns; i++){
      columns.add(Bytes.readByteArray(in));
    }
  }  
  
  public void write(final DataOutput out)
  throws IOException {
    Bytes.writeByteArray(out, this.family);
    out.writeInt(columns.size());
    for(byte [] column : columns){
      Bytes.writeByteArray(out, column);
    }
  }
  
  /**
   * Byte array comparator class.
   */
  public static class FamilyComparator implements Comparator<byte[]> {
    public FamilyComparator() {
      super();
    }
    @Override
    public int compare(byte [] left, byte [] right) {
      return compareTo(left, right);
    }
    
    private int compareTo(byte[] left, byte[] right){
      int leftLen = Bytes.toInt(left, 0, Bytes.SIZEOF_INT);
      int rightLen = Bytes.toInt(right, 0, Bytes.SIZEOF_INT);
      return Bytes.compareTo(left, Bytes.SIZEOF_INT, leftLen,
          right, Bytes.SIZEOF_INT, rightLen);
    }
  }
  
}
