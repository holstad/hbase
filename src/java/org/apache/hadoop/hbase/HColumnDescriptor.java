/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 * 
 * It is used as input when creating a table or adding a column. Once set, the
 * parameters that specify a column cannot be changed without deleting the
 * column and recreating it. If there is data stored in the column, it will be
 * deleted when the column is deleted.
 */
public class HColumnDescriptor implements WritableComparable {
  // For future backward compatibility

  // Version 3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version 5 was when bloom filter descriptors were removed.
  // Version 6 adds metadata as a map where keys and values are byte[].
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte)6;

  /** 
   * The type of compression.
   * @see org.apache.hadoop.io.SequenceFile.Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */
    NONE, 
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }

  public static final String COMPRESSION = "COMPRESSION";
  public static final String BLOCKCACHE = "BLOCKCACHE";
  public static final String LENGTH = "LENGTH";
  public static final String TTL = "TTL";
  public static final String BLOOMFILTER = "BLOOMFILTER";
  public static final String FOREVER = "FOREVER";
  public static final String MAPFILE_INDEX_INTERVAL =
      "MAPFILE_INDEX_INTERVAL";

  /**
   * Default compression type.
   */
  public static final CompressionType DEFAULT_COMPRESSION =
    CompressionType.NONE;

  /**
   * Default number of versions of a record to keep.
   */
  public static final int DEFAULT_VERSIONS = 3;

  /**
   * Default maximum cell length.
   */
  public static final int DEFAULT_LENGTH = Integer.MAX_VALUE;

  /**
   * Default setting for whether to serve from memory or not.
   */
  public static final boolean DEFAULT_IN_MEMORY = false;

  /**
   * Default setting for whether to use a block cache or not.
   */
  public static final boolean DEFAULT_BLOCKCACHE = false;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  public static final boolean DEFAULT_BLOOMFILTER = false;
  
  /**
   * Default time to live of cell contents.
   */
  public static final int DEFAULT_TTL = HConstants.FOREVER;

  // Column family name
  private byte [] name;

  /**
   * Default mapfile index interval.
   */
  public static final int DEFAULT_MAPFILE_INDEX_INTERVAL = 128;

  // Column metadata
  protected Map<ImmutableBytesWritable,ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable,ImmutableBytesWritable>();


  /**
   * Default constructor. Must be present for Writable.
   */
  public HColumnDescriptor() {
    this.name = null;
  }

  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param columnName - column family name
   */
  public HColumnDescriptor(final String columnName) {
    this(Bytes.toBytes(columnName));
  }
  
  /**
   * Construct a column descriptor specifying only the family name 
   * The other attributes are defaulted.
   * 
   * @param columnName Column family name.  Must have the ':' ending.
   */
  public HColumnDescriptor(final byte [] columnName) {
    this (columnName == null || columnName.length <= 0?
      HConstants.EMPTY_BYTE_ARRAY: columnName, DEFAULT_VERSIONS,
      DEFAULT_COMPRESSION, DEFAULT_IN_MEMORY, DEFAULT_BLOCKCACHE,
      Integer.MAX_VALUE, DEFAULT_TTL, false);
  }

  /**
   * Constructor.
   * Makes a deep copy of the supplied descriptor. 
   * Can make a modifiable descriptor from an UnmodifyableHColumnDescriptor.
   * @param desc The descriptor.
   */
  public HColumnDescriptor(HColumnDescriptor desc) {
    super();
    this.name = desc.name.clone();
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
  }

  /**
   * Constructor
   * @param columnName Column family name.  Must have the ':' ending.
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param maxValueLength Restrict values to &lt;= this value
   * @param timeToLive Time-to-live of cell contents, in seconds from last timestamp
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Enable the specified bloom filter for this column
   * 
   * @throws IllegalArgumentException if passed a family name that is made of 
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> and does not
   * end in a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   */
  public HColumnDescriptor(final byte [] columnName, final int maxVersions,
      final CompressionType compression, final boolean inMemory,
      final boolean blockCacheEnabled, final int maxValueLength,
      final int timeToLive, final boolean bloomFilter) {
    isLegalFamilyName(columnName);
    this.name = stripColon(columnName);
    if (maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }
    setMaxVersions(maxVersions);
    setInMemory(inMemory);
    setBlockCacheEnabled(blockCacheEnabled);
    setMaxValueLength(maxValueLength);
    setTimeToLive(timeToLive);
    setCompressionType(compression);
    setBloomfilter(bloomFilter);
  }
  
  private static byte [] stripColon(final byte [] n) {
    byte [] result = new byte [n.length - 1];
    // Have the stored family name be absent the colon delimiter
    System.arraycopy(n, 0, result, 0, n.length - 1);
    return result;
  }

  /**
   * @param b Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException If not null and not a legitimate family
   * name: i.e. 'printable' and ends in a ':' (Null passes are allowed because
   * <code>b</code> can be null when deserializing).
   */
  public static byte [] isLegalFamilyName(final byte [] b) {
    if (b == null) {
      return b;
    }
    if (b[b.length - 1] != ':') {
      throw new IllegalArgumentException("Family names must end in a colon: " +
        Bytes.toString(b));
    }
    for (int i = 0; i < (b.length - 1); i++) {
      if (Character.isLetterOrDigit(b[i]) || b[i] == '_' || b[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <" + b[i] +
        ">. Family names  can only contain  'word characters' and must end" +
        "with a colon: " + Bytes.toString(b));
    }
    return b;
  }

  /**
   * @return Name of this column family
   */
  public byte [] getName() {
    return name;
  }

  /**
   * @return Name of this column family with colon as required by client API
   */
  public byte [] getNameWithColon() {
    return HStoreKey.addDelimiter(this.name);
  }

  /**
   * @return Name of this column family
   */
  public String getNameAsString() {
    return Bytes.toString(this.name);
  }

  /**
   * @param key The key.
   * @return The value.
   */
  public byte[] getValue(byte[] key) {
    ImmutableBytesWritable ibw = values.get(new ImmutableBytesWritable(key));
    if (ibw == null)
      return null;
    return ibw.get();
  }

  /**
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * @return All values.
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
    return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(byte[] key, byte[] value) {
    values.put(new ImmutableBytesWritable(key),
      new ImmutableBytesWritable(value));
  }

  /**
   * @param key The key.
   * @param value The value.
   */
  public void setValue(String key, String value) {
    setValue(Bytes.toBytes(key), Bytes.toBytes(value));
  }

  /** @return compression type being used for the column family */
  public CompressionType getCompression() {
    String value = getValue(COMPRESSION);
    if (value != null) {
      if (value.equalsIgnoreCase("BLOCK"))
        return CompressionType.BLOCK;
      else if (value.equalsIgnoreCase("RECORD"))
        return CompressionType.RECORD;
    }
    return CompressionType.NONE;
  }
  
  /** @return maximum number of versions */
  public int getMaxVersions() {
    String value = getValue(HConstants.VERSIONS);
    if (value != null)
      return Integer.valueOf(value);
    return DEFAULT_VERSIONS;
  }

  /**
   * @param maxVersions maximum number of versions
   */
  public void setMaxVersions(int maxVersions) {
    setValue(HConstants.VERSIONS, Integer.toString(maxVersions));
  }
  
  /**
   * @return Compression type setting.
   */
  public CompressionType getCompressionType() {
    return getCompression();
  }

  /**
   * @param type Compression type setting.
   */
  public void setCompressionType(CompressionType type) {
    String compressionType;
    switch (type) {
      case BLOCK:  compressionType = "BLOCK";   break;
      case RECORD: compressionType = "RECORD";  break;
      default:     compressionType = "NONE";    break;
    }
    setValue(COMPRESSION, compressionType);
  }

  /**
   * @return True if we are to keep all in use HRegionServer cache.
   */
  public boolean isInMemory() {
    String value = getValue(HConstants.IN_MEMORY);
    if (value != null)
      return Boolean.valueOf(value);
    return DEFAULT_IN_MEMORY;
  }
  
  /**
   * @param inMemory True if we are to keep all values in the HRegionServer
   * cache
   */
  public void setInMemory(boolean inMemory) {
    setValue(HConstants.IN_MEMORY, Boolean.toString(inMemory));
  }

  /**
   * @return Maximum value length.
   */
  public int getMaxValueLength() {
    String value = getValue(LENGTH);
    if (value != null)
      return Integer.valueOf(value);
    return DEFAULT_LENGTH;
  }

  /**
   * @param maxLength Maximum value length.
   */
  public void setMaxValueLength(int maxLength) {
    setValue(LENGTH, Integer.toString(maxLength));
  }

  /**
   * @return Time to live.
   */
  public int getTimeToLive() {
    String value = getValue(TTL);
    if (value != null)
      return Integer.valueOf(value);
    return DEFAULT_TTL;
  }

  /**
   * @param timeToLive
   */
  public void setTimeToLive(int timeToLive) {
    setValue(TTL, Integer.toString(timeToLive));
  }

  /**
   * @return True if MapFile blocks should be cached.
   */
  public boolean isBlockCacheEnabled() {
    String value = getValue(BLOCKCACHE);
    if (value != null)
      return Boolean.valueOf(value);
    return DEFAULT_BLOCKCACHE;
  }

  /**
   * @param blockCacheEnabled True if MapFile blocks should be cached.
   */
  public void setBlockCacheEnabled(boolean blockCacheEnabled) {
    setValue(BLOCKCACHE, Boolean.toString(blockCacheEnabled));
  }

  /**
   * @return true if a bloom filter is enabled
   */
  public boolean isBloomfilter() {
    String value = getValue(BLOOMFILTER);
    if (value != null)
      return Boolean.valueOf(value);
    return DEFAULT_BLOOMFILTER;
  }

  /**
   * @param onOff Enable/Disable bloom filter
   */
  public void setBloomfilter(final boolean onOff) {
    setValue(BLOOMFILTER, Boolean.toString(onOff));
  }

  /**
   * @return The number of entries that are added to the store MapFile before
   * an index entry is added.
   */
  public int getMapFileIndexInterval() {
    String value = getValue(MAPFILE_INDEX_INTERVAL);
    if (value != null)
      return Integer.valueOf(value);
    return DEFAULT_MAPFILE_INDEX_INTERVAL;
  }

  /**
   * @param interval The number of entries that are added to the store MapFile before
   * an index entry is added.
   */
  public void setMapFileIndexInterval(int interval) {
    setValue(MAPFILE_INDEX_INTERVAL, Integer.toString(interval));
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    StringBuffer s = new StringBuffer();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      s.append(", ");
      s.append(Bytes.toString(e.getKey().get()));
      s.append(" => '");
      s.append(Bytes.toString(e.getValue().get()));
      s.append("'");
    }
    s.append('}');
    return s.toString();
  }
  
  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return compareTo(obj) == 0;
  }
  
  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.name);
    result ^= Byte.valueOf(COLUMN_DESCRIPTOR_VERSION).hashCode();
    result ^= values.hashCode();
    return result;
  }
  
  // Writable

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version < 6) {
      if (version <= 2) {
        Text t = new Text();
        t.readFields(in);
        this.name = t.getBytes();
        if (HStoreKey.getFamilyDelimiterIndex(this.name) > 0) {
          this.name = stripColon(this.name);
        }
      } else {
        this.name = Bytes.readByteArray(in);
      }
      this.values.clear();
      setMaxVersions(in.readInt());
      int ordinal = in.readInt();
      setCompressionType(CompressionType.values()[ordinal]);
      setInMemory(in.readBoolean());
      setMaxValueLength(in.readInt());
      setBloomfilter(in.readBoolean());
      if (isBloomfilter() && version < 5) {
        // If a bloomFilter is enabled and the column descriptor is less than
        // version 5, we need to skip over it to read the rest of the column
        // descriptor. There are no BloomFilterDescriptors written to disk for
        // column descriptors with a version number >= 5
        BloomFilterDescriptor junk = new BloomFilterDescriptor();
        junk.readFields(in);
      }
      if (version > 1) {
        setBlockCacheEnabled(in.readBoolean());
      }
      if (version > 2) {
       setTimeToLive(in.readInt());
      }
    } else {
      // version 6+
      this.name = Bytes.readByteArray(in);
      this.values.clear();
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        key.readFields(in);
        value.readFields(in);
        values.put(key, value);
      }
    }
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeByte(COLUMN_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, this.name);
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
  }

  // Comparable

  /** {@inheritDoc} */
  public int compareTo(Object o) {
    HColumnDescriptor other = (HColumnDescriptor)o;
    int result = Bytes.compareTo(this.name, other.getName());
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference
      result = this.values.hashCode() - other.values.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    return result;
  }
}