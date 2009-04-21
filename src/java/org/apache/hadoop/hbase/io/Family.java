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
  private byte [] family = new byte [0];
//  protected List<byte[]> columns = new ArrayList<byte[]>();
  private List<byte[]> columns = new ArrayList<byte[]>();
//  protected final byte[] ZERO_BYTES = new byte[]{(byte)0};
  //This list is only used for temporary storage before the puts are converted
  //into a KeyValue when sending the update
  private List<byte[]> values = null;
  
  public Family(){}
  public Family(byte[] family){
    this.family = family;
  }
  public Family(byte[] family, byte[] column) {
    this.family = family;
    this.columns.add(column);
  }
  public Family(byte[] family, List<byte[]> columns) {
    this.family = family;
    this.columns = columns;
  }
  
  //Puts
  public Family(byte[] family, byte[] column, byte[] value) {
    this.family = family;
    this.columns.add(column);
    this.values = new ArrayList<byte[]>();
    this.values.add(value);
  }
  public Family(byte[] family, List<byte[]> columns, List<byte[]> values) {
    this.family = family;
    this.columns = columns;
    this.values = values;
  }
  

  public void add(byte[] column){
    this.columns.add(column);
  }
  public void add(byte[] column, byte[] value){
    this.columns.add(column);
    this.values.add(value);
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
  
  
  //TODO have to check how those sorts
  public void sortColumns(){
    byte[][] cols = new byte[columns.size()][];
    cols = columns.toArray(new byte[0][]);
    Arrays.sort(cols, Bytes.BYTES_COMPARATOR);
    columns.clear();
    for(byte[] bytes : cols){
      columns.add(bytes);
    }
  }
  
  public void createKeyValuesFromColumns(byte[] row, final long ts){
    List<byte[]> updatedColumns = new ArrayList<byte[]>(columns.size());
    for(int i=0; i<columns.size(); i++){
      updatedColumns.add(new KeyValue(row, this.family, columns.get(i),
          ts, KeyValue.Type.Put, values.get(i)).getBuffer());
    }
    columns = updatedColumns;
  }
  
  
  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();
    sb.append("Family ");
    sb.append(new String(family));
    sb.append(", columns[");
    int i = 0;
    for(; i<columns.size()-1; i++){
      sb.append(new String(columns.get(i)));
      sb.append(", ");      
    }
    sb.append(new String(columns.get(i)));
    sb.append("]"); 
    return sb.toString();
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
