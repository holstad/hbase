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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.znerd.xmlenc.LineBreak;
import org.znerd.xmlenc.XMLOutputter;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * GenericHandler contains some basic common stuff that all the individual
 * REST handler types take advantage of.
 */
public abstract class GenericHandler {
  protected HBaseConfiguration conf;
  protected HBaseAdmin admin;

  protected static final String ACCEPT = "accept";
  protected static final String COLUMN = "column";
  protected static final String TIMESTAMP = "timestamp";
  protected static final String START_ROW = "start_row";
  protected static final String END_ROW = "end_row";
  protected static final String CONTENT_TYPE = "content-type";
  protected static final String ROW = "row";
  protected static final String REGIONS = "regions";
  protected static final String VERSION = "version";
  protected static final String OFFSET = "offset";
  protected static final String LIMIT = "limit";
  
  protected final Log LOG = LogFactory.getLog(this.getClass());

  public GenericHandler(HBaseConfiguration conf, HBaseAdmin admin)  {
    this.conf = conf;
    this.admin = admin;
  }

  /*
   * Supported content types as enums
   */
  protected enum ContentType {
    XML("text/xml"),
    PLAIN("text/plain"),
    MIME("multipart/related"),
    NOT_ACCEPTABLE("");
    
    private final String type;
    
    private ContentType(final String t) {
      this.type = t;
    }
    
    @Override
    public String toString() {
      return this.type;
    }
    
    /**
     * Utility method used looking at Accept header content.
     * @param t The content type to examine.
     * @return The enum that matches the prefix of <code>t</code> or
     * the default enum if <code>t</code> is empty.  If unsupported type, we
     * return NOT_ACCEPTABLE.
     */
    public static ContentType getContentType(final String t) {
      // Default to text/plain. Curl sends */*.
      if (t == null || t.equals("*/*")) { 
        return ContentType.XML;
      }
      String lowerCased = t.toLowerCase();
      ContentType [] values = ContentType.values();
      ContentType result = null;
      for (int i = 0; i < values.length; i++) {
        if (lowerCased.startsWith(values[i].type)) {
          result = values[i];
          break;
        }
      }
      return result == null? NOT_ACCEPTABLE: result;
    }
  }

  
  /*
   * @param o
   * @return XMLOutputter wrapped around <code>o</code>.
   * @throws IllegalStateException
   * @throws IOException
   */
  protected XMLOutputter getXMLOutputter(final PrintWriter o)
  throws IllegalStateException, IOException {
    XMLOutputter outputter = new XMLOutputter(o, HConstants.UTF8_ENCODING);
    outputter.setLineBreak(LineBreak.UNIX);
    outputter.setIndentation(" ");
    outputter.declaration();
    return outputter;
  }
  
  /*
   * Write an XML element.
   * @param outputter
   * @param name
   * @param value
   * @throws IllegalStateException
   * @throws IOException
   */
  protected void doElement(final XMLOutputter outputter,
      final String name, final String value)
  throws IllegalStateException, IOException {
    outputter.startTag(name);
    if (value.length() > 0) {
      outputter.pcdata(value);
    }
    outputter.endTag();
  }
  
  /*
   * Set content-type, encoding, and status on passed <code>response</code>
   * @param response
   * @param status
   * @param contentType
   */
  public static void setResponseHeader(final HttpServletResponse response,
      final int status, final String contentType) {
    // Container adds the charset to the HTTP content-type header.
    response.setContentType(contentType);
    response.setCharacterEncoding(HConstants.UTF8_ENCODING);
    response.setStatus(status);
  }

  /*
   * If we can't do the specified Accepts header type.
   * @param response
   * @throws IOException
   */
  public static void doNotAcceptable(final HttpServletResponse response)
  throws IOException {
    response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE);
  }

  /*
   * If we can't do the specified Accepts header type.
   * @param response
   * @param message
   * @throws IOException
   */
  public static void doNotAcceptable(final HttpServletResponse response,
      final String message)
  throws IOException {
    response.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, message);
  }
  
  /*
   * Resource not found.
   * @param response
   * @throws IOException
   */
  public static void doNotFound(final HttpServletResponse response)
  throws IOException {
    response.sendError(HttpServletResponse.SC_NOT_FOUND);
  }
  
  /*
   * Resource not found.
   * @param response
   * @param msg
   * @throws IOException
   */
  public static void doNotFound(final HttpServletResponse response, final String msg)
  throws IOException {
    response.sendError(HttpServletResponse.SC_NOT_FOUND, msg);
  }

  /*
   * Unimplemented method.
   * @param response
   * @param message to send
   * @throws IOException
   */
  public static void doMethodNotAllowed(final HttpServletResponse response,
      final String message)
  throws IOException {
    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, message);
  }

  protected String getTableName(final String [] pathSegments)
  throws UnsupportedEncodingException {
    // Get table name?  First part of passed segment.  It can't be empty string
    // or null because we should have tested for that before coming in here.
    return URLDecoder.decode(pathSegments[0], HConstants.UTF8_ENCODING);
  }
 
  /*
   * Output row columns 
   * @param outputter
   * @param m
   * @throws IllegalStateException
   * @throws IllegalArgumentException
   * @throws IOException
   */
  protected void outputColumnsXml(final XMLOutputter outputter, final Map<byte [], Cell> m)
  throws IllegalStateException, IllegalArgumentException, IOException {
    outputColumnsXml(null, outputter, m);
  }
  
  protected void outputColumnsXml(final HttpServletRequest request,
      final XMLOutputter outputter, final Map<byte [], Cell> m)
  throws IllegalStateException, IllegalArgumentException, IOException {
    int offset = 0, limit = Integer.MAX_VALUE;
    if (request != null) {
      String offset_string = request.getParameter(OFFSET);
      if (offset_string != null && !offset_string.equals(""))
        offset = Integer.parseInt(offset_string);
      String limit_string = request.getParameter(LIMIT);
      if (limit_string != null && !limit_string.equals("")) {
        limit = Integer.parseInt(limit_string);
      }
    }
    
    for (Map.Entry<byte [], Cell> e: m.entrySet()) {
      if (offset > 0) {
        --offset;
        continue;
      }
      if (limit < 1) {
        break;
      } else {
        --limit;
      }
      outputter.startTag(COLUMN);
      doElement(outputter, "name", 
        org.apache.hadoop.hbase.util.Base64.encodeBytes(e.getKey()));
      outputCellXml(outputter, e.getValue());
      outputter.endTag();
    }
  }

  protected void outputColumnsWithMultiVersionsXml(final XMLOutputter outputter, 
      final Map<byte [], Cell[]> m)
  throws IllegalStateException, IllegalArgumentException, IOException {
    outputColumnsWithMultiVersionsXml(null, outputter, m);
  }
  
  protected void outputColumnsWithMultiVersionsXml(final HttpServletRequest request, 
      final XMLOutputter outputter, final Map<byte [], Cell[]> m)
  throws IllegalStateException, IllegalArgumentException, IOException {
    int offset = 0, limit = Integer.MAX_VALUE;
    if (request != null) {
      String offset_string = request.getParameter(OFFSET);
      if (offset_string != null && !offset_string.equals(""))
        offset = Integer.parseInt(offset_string);
      String limit_string = request.getParameter(LIMIT);
      if (limit_string != null && !limit_string.equals("")) {
        limit = Integer.parseInt(limit_string);
      }
    }
    
    for (Map.Entry<byte [], Cell[]> e: m.entrySet()) {
      if (offset > 0) {
        --offset;
        continue;
      }
      if (limit < 1) {
        break;
      } else {
        --limit;
      }
      for (Cell c : e.getValue()) {
        outputter.startTag(COLUMN);
        doElement(outputter, "name", 
            org.apache.hadoop.hbase.util.Base64.encodeBytes(e.getKey())); 
        outputCellXml(outputter, c);
        outputter.endTag();       
      }
    }
  }
  
  protected void outputCellXml(final XMLOutputter outputter, Cell c) 
  throws IllegalStateException, IllegalArgumentException, IOException {
    // We don't know String from binary data so we always base64 encode.
    doElement(outputter, "value",
        org.apache.hadoop.hbase.util.Base64.encodeBytes(c.getValue()));
    doElement(outputter, "timestamp", String.valueOf(c.getTimestamp()));    
  }
//  Commented - multipart support is currently nonexistant.
//  protected void outputColumnsMime(final MultiPartResponse mpr,
//     final Map<Text, Cell> m)
//   throws IOException {
//     for (Map.Entry<Text, Cell> e: m.entrySet()) {
//       mpr.startPart("application/octet-stream",
//         new String [] {"Content-Description: " + e.getKey().toString(),
//           "Content-Transfer-Encoding: binary",
//           "Content-Length: " + e.getValue().getValue().length});
//       mpr.getOut().write(e.getValue().getValue());
//     }  
//   }
 
  /*
   * Get an HTable instance by it's table name.
   */
  protected HTable getTable(final String tableName) throws IOException {
    return new HTable(this.conf, Bytes.toBytes(tableName));
  }
  
  protected String makeColumnName(String column) {
   if (column.indexOf(':') == -1)
     column += ':';
   return column;
  }
}
