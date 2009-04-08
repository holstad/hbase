package org.apache.hadoop.hbase.io;

//import java.util.Comparator;
import java.util.TreeMap;
import java.util.Set;

//import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.RowFilterInterface;;

/**
 * Interface for all the different get calls 
 * 
 */
public interface Get {

  public Family[] getFamilies();
  public void setFamilies(Set<byte [] > families);

  public RowFilterInterface getFilter();
  public void setFilter(RowFilterInterface filter);

  public byte [] getRow();
  
  public TimeRange getTimeRange();
  
  public int getVersions();
  public void setVersions(int versions);
  
//  public void setComparator(Comparator comp);
//  public void setMap(TreeMap<byte[], Integer> treemap);
  
 
//  // Type is set to the query type, for example 0 for getRow, 1 for getFamilies
//  // 2 for getColumns and 3 for getTop
//  int type = 0;
//  
//  byte [] row = null;
//  Family [] families = null;
//  int versions = 0;
//  TimeRange tr = null;
//  int topNumber = 0;
//  
//
//  
//  public Get(int type, byte [] row, Family [] families, int versions, TimeRange tr,
//      int topNumber){
//    this.type = type;
//    this.row = row;
//    this.families = families;
//    this.versions = versions;
//    this.tr = tr;
//    this.topNumber = topNumber;
//  }
//
//  public byte [] getRow(){
//    return row;
//  }
//  
//  public Family [] getFamilies(){
//    return families;
//  }
//  public void setFamilies(Family[] families){
//    this.families = families;
//  }
//  public void setFamilies(Set<byte[]> families){
//    int famLen = families.size();
//    this.families = new Family[famLen]; 
//    int i = 0;
//    for(byte [] family  : families){
//      this.families[i] = new Family(family);
//      i++;
//    }
//  }
//
//  public int getVersions(){
//    return versions;
//  }
//  
//  public TimeRange getTimeRange(){
//    return tr;
//  }
//  
//  public int getTopNumber(){
//    return topNumber;
//  }
//  
//  public int getType(){
//    return type;
//  }
}
