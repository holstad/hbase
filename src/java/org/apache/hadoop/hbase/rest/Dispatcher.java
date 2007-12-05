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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.mortbay.http.SocketListener;

import java.io.File;
import java.io.FileNotFoundException;

import java.net.URL;
import org.mortbay.http.HttpContext;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.InfoServer;

/**
 * Servlet implementation class for hbase REST interface.
 * Presumes container ensures single thread through here at any one time
 * (Usually the default configuration).  In other words, code is not
 * written thread-safe.
 * <p>This servlet has explicit dependency on Jetty server; it uses the
 * jetty implementation of MultipartResponse.
 * 
 * <p>TODO:
 * <ul>
 * <li>multipart/related response is not correct; the servlet setContentType
 * is broken.  I am unable to add parameters such as boundary or start to
 * multipart/related.  They get stripped.</li>
 * <li>Currently creating a scanner, need to specify a column.  Need to make
 * it so the HTable instance has current table's metadata to-hand so easy to
 * find the list of all column families so can make up list of columns if none
 * specified.</li>
 * <li>Minor items are we are decoding URLs in places where probably already
 * done and how to timeout scanners that are in the scanner list.</li>
 * </ul>
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseRest">Hbase REST Specification</a>
 */
public class Dispatcher extends javax.servlet.http.HttpServlet
implements javax.servlet.Servlet {
  private MetaHandler metaHandler;
  private TableHandler tableHandler;
  private ScannerHandler scannerHandler;

  private static final String SCANNER = "scanner";
  private static final String ROW = "row";
      
  /**
   * Default constructor
   */
  public Dispatcher() {
    super();
  }

  public void init() throws ServletException {
    super.init();
    
    HBaseConfiguration conf = new HBaseConfiguration();
    HBaseAdmin admin = null;
    
    try{
      admin = new HBaseAdmin(conf);
      metaHandler = new MetaHandler(conf, admin);
      tableHandler = new TableHandler(conf, admin);
      scannerHandler = new ScannerHandler(conf, admin);
    } catch(Exception e){
      throw new ServletException(e);
    }
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 0 || pathSegments[0].length() <= 0) {
      // if it was a root request, then get some metadata about 
      // the entire instance.
      metaHandler.doGet(request, response, pathSegments);
    } else {
      // otherwise, it must be a GET request suitable for the
      // table handler.
      tableHandler.doGet(request, response, pathSegments);
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    // there should be at least two path segments (table name and row or scanner)
    if (pathSegments.length >= 2 && pathSegments[0].length() > 0) {
      if (pathSegments[1].toLowerCase().equals(SCANNER) &&
          pathSegments.length >= 2) {
        scannerHandler.doPost(request, response, pathSegments);
        return;
      } else if (pathSegments[1].toLowerCase().equals(ROW) && pathSegments.length >= 3) {
        tableHandler.doPost(request, response, pathSegments);
        return;
      }
    }

    // if we get to this point, then no handler was matched this request.
    GenericHandler.doNotFound(response, "No handler for " + request.getPathInfo());
  }
  

  protected void doPut(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
    // Equate PUT with a POST.
    doPost(request, response);
  }

  protected void doDelete(HttpServletRequest request,
      HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    // must be at least two path segments (table name and row or scanner)
    if (pathSegments.length >= 2 && pathSegments[0].length() > 0) {
      // DELETE to a scanner requires at least three path segments
      if (pathSegments[1].toLowerCase().equals(SCANNER) &&
          pathSegments.length == 3 && pathSegments[2].length() > 0) {
        scannerHandler.doDelete(request, response, pathSegments);
        return;
      } else if (pathSegments[1].toLowerCase().equals(ROW) &&
          pathSegments.length >= 3) {
        tableHandler.doDelete(request, response, pathSegments);
        return;
      } 
    }
    
    // if we reach this point, then no handler exists for this request.
    GenericHandler.doNotFound(response, "No handler");
  }
  
  /*
   * @param request
   * @return request pathinfo split on the '/' ignoring the first '/' so first
   * element in pathSegment is not the empty string.
   */
  private String [] getPathSegments(final HttpServletRequest request) {
    int context_len = request.getContextPath().length() + 1;
    return request.getRequestURI().substring(context_len).split("/");
  }


  /*
   * Start up the REST servlet in standalone mode.
   */
  public static void main(String[] args) throws Exception{
    int port = 60050;
    String bindAddress = "0.0.0.0";
    
    // grab the port and bind addresses from the command line if supplied
    for(int i = 0; i < args.length; i++){
      if(args[i].equals("--port")){
        port = Integer.parseInt(args[++i]);
      } else if(args[i].equals("--bind")){
        bindAddress = args[++i];
      } else if(args[i].equals("--help")){
        printUsage();
        return;
      } else {
        System.out.println("Unrecognized switch " + args[i]);
        printUsage();
        return;
      }
    }
    
    org.mortbay.jetty.Server webServer = new org.mortbay.jetty.Server();

    SocketListener listener = new SocketListener();
    listener.setPort(port);
    listener.setHost(bindAddress);
    webServer.addListener(listener);
  
    webServer.addWebApplication("/api", InfoServer.getWebAppDir("rest"));
    
    webServer.start();
  }  
  
  /*
   * Print out the usage of this class from the command line.
   */ 
  private static void printUsage(){
    System.out.println("Start up the HBase REST servlet.");
    System.out.println("Options:");
    System.out.println("--port [port]");
    System.out.println("\tPort to listen on. Defaults to 60050.");
    System.out.println("--bind [addr]");
    System.out.println("\tAddress to bind on. Defaults to 0.0.0.0.");
    System.out.println("--help");
    System.out.println("\tPrint this message and exit.");
  }
}
