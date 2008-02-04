/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataInputBuffer;

/**
 * An implementation of {@link FSInputStream} that reads the stream in blocks
 * of a fixed, configurable size. The blocks are stored in a memory-sensitive cache.
 */
public class BlockFSInputStream extends FSInputStream {
  
  static final Log LOG = LogFactory.getLog(BlockFSInputStream.class);
  
  private final InputStream in;

  private final long fileLength;

  private final int blockSize;
  private final Map<Long, byte[]> blocks;

  private boolean closed;

  private DataInputBuffer blockStream = new DataInputBuffer();

  private long blockEnd = -1;

  private long pos = 0;

  /**
   * @param in
   * @param fileLength
   * @param blockSize the size of each block in bytes.
   */
  @SuppressWarnings("unchecked")
  public BlockFSInputStream(InputStream in, long fileLength, int blockSize) {
    this.in = in;
    if (!(in instanceof Seekable) || !(in instanceof PositionedReadable)) {
      throw new IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
    this.fileLength = fileLength;
    this.blockSize = blockSize;
    // a memory-sensitive map that has soft references to values
    this.blocks = new ReferenceMap() {
      private long hits, misses;
      @Override
      public Object get(Object key) {
        Object value = super.get(key);
        if (value == null) {
          misses++;
        } else {
          hits++;
        }
        if (LOG.isDebugEnabled() && ((hits + misses) % 10000) == 0) {
          long hitRate = (100 * hits) / (hits + misses);
          LOG.info("Hit rate for cache " + hashCode() + ": " + hitRate + "%");
        }
        return value;
      }
    };
  }

  @Override
  public synchronized long getPos() throws IOException {
    return pos;
  }

  @Override
  public synchronized int available() throws IOException {
    return (int) (fileLength - pos);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > fileLength) {
      throw new IOException("Cannot seek after EOF");
    }
    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    int result = -1;
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      result = blockStream.read();
      if (result >= 0) {
        pos++;
      }
    }
    return result;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      int realLen = Math.min(len, (int) (blockEnd - pos + 1));
      int result = blockStream.read(buf, off, realLen);
      if (result >= 0) {
        pos += result;
      }
      return result;
    }
    return -1;
  }

  private synchronized void blockSeekTo(long target) throws IOException {
    int targetBlock = (int) (target / blockSize);
    long targetBlockStart = targetBlock * blockSize;
    long targetBlockEnd = Math.min(targetBlockStart + blockSize, fileLength) - 1;
    long blockLength = targetBlockEnd - targetBlockStart + 1;
    long offsetIntoBlock = target - targetBlockStart;

    byte[] block = blocks.get(targetBlockStart);
    if (block == null) {
      block = new byte[blockSize];
      ((PositionedReadable) in).readFully(targetBlockStart, block, 0,
          (int) blockLength);
      blocks.put(targetBlockStart, block);
    }
    
    this.pos = target;
    this.blockEnd = targetBlockEnd;
    this.blockStream.reset(block, (int) offsetIntoBlock,
        (int) (blockLength - offsetIntoBlock));

  }

  @Override
  public void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (blockStream != null) {
      blockStream.close();
      blockStream = null;
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @Override
  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }

}
