/**
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

package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/** 
 * This is a customized version of the polymorphic hadoop
 * {@link ObjectWritable}.  It removes UTF8 (HADOOP-414).
 * Using {@link Text} intead of UTF-8 saves ~2% CPU between reading and writing
 * objects running a short sequentialWrite Performance Evaluation test just in
 * ObjectWritable alone; more when we're doing randomRead-ing.  Other
 * optimizations include our passing codes for classes instead of the
 * actual class names themselves.  This makes it so this class needs amendment
 * if non-Writable classes are introduced -- if passed a Writable for which we
 * have no code, we just do the old-school passing of the class name, etc. --
 * but passing codes the  savings are large particularly when cell
 * data is small (If < a couple of kilobytes, the encoding/decoding of class
 * name and reflection to instantiate class was costing in excess of the cell
 * handling).
 */
public class HbaseObjectWritable implements Writable, Configurable {
  protected final static Log LOG = LogFactory.getLog(HbaseObjectWritable.class);
  
  // Here we maintain two static maps of classes to code and vice versa.
  // Add new classes+codes as wanted or figure way to auto-generate these
  // maps from the HMasterInterface.
  static final Map<Byte, Class<?>> CODE_TO_CLASS =
    new HashMap<Byte, Class<?>>();
  static final Map<Class<?>, Byte> CLASS_TO_CODE =
    new HashMap<Class<?>, Byte>();
  // Special code that means 'not-encoded'; in this case we do old school
  // sending of the class name using reflection, etc.
  private static final byte NOT_ENCODED = 0;
  static {
    byte code = NOT_ENCODED + 1;
    // Primitive types.
    addToMap(Boolean.TYPE, code++);
    addToMap(Byte.TYPE, code++);
    addToMap(Character.TYPE, code++);
    addToMap(Short.TYPE, code++);
    addToMap(Integer.TYPE, code++);
    addToMap(Long.TYPE, code++);
    addToMap(Float.TYPE, code++);
    addToMap(Double.TYPE, code++);
    addToMap(Void.TYPE, code++);
    // Other java types
    addToMap(String.class, code++);
    addToMap(byte [].class, code++);
    // Hadoop types
    addToMap(Text.class, code++);
    addToMap(Writable.class, code++);
    addToMap(MapWritable.class, code++);
    addToMap(NullInstance.class, code++);
    try {
      addToMap(Class.forName("[Lorg.apache.hadoop.io.Text;"), code++);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    // Hbase types
    addToMap(HServerInfo.class, code++);
    addToMap(HMsg.class, code++);
    addToMap(HTableDescriptor.class, code++);
    addToMap(HColumnDescriptor.class, code++);
    addToMap(RowFilterInterface.class, code++);
    addToMap(RowFilterSet.class, code++);
    addToMap(HRegionInfo.class, code++);
    addToMap(BatchUpdate.class, code++);
    addToMap(HServerAddress.class, code++);
    addToMap(HRegionInfo.class, code++);
    try {
      addToMap(Class.forName("[Lorg.apache.hadoop.hbase.HMsg;"), code++);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  private Class<?> declaredClass;
  private Object instance;
  private Configuration conf;

  public HbaseObjectWritable() {
    super();
  }
  
  public HbaseObjectWritable(Object instance) {
    set(instance);
  }

  public HbaseObjectWritable(Class<?> declaredClass, Object instance) {
    this.declaredClass = declaredClass;
    this.instance = instance;
  }

  /** Return the instance, or null if none. */
  public Object get() { return instance; }
  
  /** Return the class this is meant to be. */
  public Class<?> getDeclaredClass() { return declaredClass; }
  
  /** Reset the instance. */
  public void set(Object instance) {
    this.declaredClass = instance.getClass();
    this.instance = instance;
  }
  
  public String toString() {
    return "OW[class=" + declaredClass + ",value=" + instance + "]";
  }

  
  public void readFields(DataInput in) throws IOException {
    readObject(in, this, this.conf);
  }
  
  public void write(DataOutput out) throws IOException {
    writeObject(out, instance, declaredClass, conf);
  }

  private static class NullInstance extends Configured implements Writable {
    Class<?> declaredClass;
    public NullInstance() { super(null); }
    
    public NullInstance(Class<?> declaredClass, Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    
    @SuppressWarnings("boxing")
    public void readFields(DataInput in) throws IOException {
      this.declaredClass = CODE_TO_CLASS.get(in.readByte());
    }
    
    public void write(DataOutput out) throws IOException {
      writeClassCode(out, this.declaredClass);
    }
  }
  
  /**
   * Write out the code byte for passed Class.
   * @param out
   * @param c
   * @throws IOException
   */
  @SuppressWarnings("boxing")
  static void writeClassCode(final DataOutput out, final Class<?> c)
  throws IOException {
    Byte code = CLASS_TO_CODE.get(c);
    if (code == null) {
      LOG.error("Unsupported type " + c);
      throw new UnsupportedOperationException("No code for unexpected " + c);
    }
    out.writeByte(code);
  }

  /** Write a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings({ "boxing", "unchecked" })
  public static void writeObject(DataOutput out, Object instance,
                                 Class declaredClass, 
                                 Configuration conf)
  throws IOException {
    if (instance == null) {                       // null
      instance = new NullInstance(declaredClass, conf);
      declaredClass = Writable.class;
    }
    writeClassCode(out, declaredClass);
    if (declaredClass.isArray()) {                // array
      int length = Array.getLength(instance);
      out.writeInt(length);
      for (int i = 0; i < length; i++) {
        writeObject(out, Array.get(instance, i),
                    declaredClass.getComponentType(), conf);
      }
    } else if (declaredClass == String.class) {   // String
      Text.writeString(out, (String)instance);
    } else if (declaredClass.isPrimitive()) {     // primitive type
      if (declaredClass == Boolean.TYPE) {        // boolean
        out.writeBoolean(((Boolean)instance).booleanValue());
      } else if (declaredClass == Character.TYPE) { // char
        out.writeChar(((Character)instance).charValue());
      } else if (declaredClass == Byte.TYPE) {    // byte
        out.writeByte(((Byte)instance).byteValue());
      } else if (declaredClass == Short.TYPE) {   // short
        out.writeShort(((Short)instance).shortValue());
      } else if (declaredClass == Integer.TYPE) { // int
        out.writeInt(((Integer)instance).intValue());
      } else if (declaredClass == Long.TYPE) {    // long
        out.writeLong(((Long)instance).longValue());
      } else if (declaredClass == Float.TYPE) {   // float
        out.writeFloat(((Float)instance).floatValue());
      } else if (declaredClass == Double.TYPE) {  // double
        out.writeDouble(((Double)instance).doubleValue());
      } else if (declaredClass == Void.TYPE) {    // void
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isEnum()) {         // enum
      Text.writeString(out, ((Enum)instance).name());
    } else if (Writable.class.isAssignableFrom(declaredClass)) { // Writable
      Class <?> c = instance.getClass();
      Byte code = CLASS_TO_CODE.get(c);
      if (code == null) {
        out.writeByte(NOT_ENCODED);
        Text.writeString(out, c.getName());
      } else {
        writeClassCode(out, c);
      }
      ((Writable)instance).write(out);
    } else {
      throw new IOException("Can't write: "+instance+" as "+declaredClass);
    }
  }
  
  
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  public static Object readObject(DataInput in, Configuration conf)
    throws IOException {
    return readObject(in, null, conf);
  }
    
  /** Read a {@link Writable}, {@link String}, primitive type, or an array of
   * the preceding. */
  @SuppressWarnings({ "unchecked", "boxing" })
  public static Object readObject(DataInput in,
      HbaseObjectWritable objectWritable, Configuration conf)
  throws IOException {
    Class<?> declaredClass = CODE_TO_CLASS.get(in.readByte());
    Object instance;
    if (declaredClass.isPrimitive()) {            // primitive types
      if (declaredClass == Boolean.TYPE) {             // boolean
        instance = Boolean.valueOf(in.readBoolean());
      } else if (declaredClass == Character.TYPE) {    // char
        instance = Character.valueOf(in.readChar());
      } else if (declaredClass == Byte.TYPE) {         // byte
        instance = Byte.valueOf(in.readByte());
      } else if (declaredClass == Short.TYPE) {        // short
        instance = Short.valueOf(in.readShort());
      } else if (declaredClass == Integer.TYPE) {      // int
        instance = Integer.valueOf(in.readInt());
      } else if (declaredClass == Long.TYPE) {         // long
        instance = Long.valueOf(in.readLong());
      } else if (declaredClass == Float.TYPE) {        // float
        instance = Float.valueOf(in.readFloat());
      } else if (declaredClass == Double.TYPE) {       // double
        instance = Double.valueOf(in.readDouble());
      } else if (declaredClass == Void.TYPE) {         // void
        instance = null;
      } else {
        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
      }
    } else if (declaredClass.isArray()) {              // array
      int length = in.readInt();
      instance = Array.newInstance(declaredClass.getComponentType(), length);
      for (int i = 0; i < length; i++) {
        Array.set(instance, i, readObject(in, conf));
      }
    } else if (declaredClass == String.class) {        // String
      instance = Text.readString(in);
    } else if (declaredClass.isEnum()) {         // enum
      instance = Enum.valueOf((Class<? extends Enum>) declaredClass,
        Text.readString(in));
    } else {                                      // Writable
      Class<?> instanceClass = null;
      Byte b = in.readByte();
      if (b.byteValue() == NOT_ENCODED) {
        String className = Text.readString(in);
        try {
          instanceClass = conf.getClassByName(className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Can't find class " + className);
        }
      } else {
        instanceClass = CODE_TO_CLASS.get(b);
      }
      Writable writable = WritableFactories.newInstance(instanceClass, conf);
      writable.readFields(in);
      instance = writable;
      if (instanceClass == NullInstance.class) {  // null
        declaredClass = ((NullInstance)instance).declaredClass;
        instance = null;
      }
    }
    if (objectWritable != null) {                 // store values
      objectWritable.declaredClass = declaredClass;
      objectWritable.instance = instance;
    }
    return instance;
  }

  @SuppressWarnings("boxing")
  private static void addToMap(final Class<?> clazz, final byte code) {
    CLASS_TO_CODE.put(clazz, code);
    CODE_TO_CLASS.put(code, clazz);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
}