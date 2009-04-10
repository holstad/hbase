package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

public class GetRow extends AbstractGet { // implements Get {

  public GetRow(byte [] row, int versions, TimeRange tr){
    super.row = row;
    super.versions = versions;
    super.families = new Family[]{new Family()};
    super.tr = tr;
  }

  public void setFamilies(Set<byte []> families){
    int famSize = families.size();
    this.families = new Family[famSize];
    Iterator<byte[]> iter = families.iterator();
    int i=0;
    while(iter.hasNext()){
      this.families[i] = new Family(iter.next());
      i++;
    }
  }
  
  public void readFields(final DataInput in) throws IOException {
    // Clear any existing operations; may be hangovers from previous use of
    // this instance.
    if (this.operations.size() != 0) {
      this.operations.clear();
    }
    this.row = Bytes.readByteArray(in);
    timestamp = in.readLong();
    this.size = in.readLong();
    int nOps = in.readInt();
    for (int i = 0; i < nOps; i++) {
      BatchOperation op = new BatchOperation();
      op.readFields(in);
      this.operations.add(op);
    }
    this.rowLock = in.readLong();
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, super.row);
    out.writeInt(super.versions);
    out.writeInt(super.families.length);
    for(Family fam : super.families){
      out.write(fam);
    }
    out.write(super.tr);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    out.writeLong(timestamp);
    out.writeLong(this.size);
    out.writeInt(operations.size());
    for (BatchOperation op: operations) {
      op.write(out);
    }
    out.writeLong(this.rowLock);
  }

  
}
