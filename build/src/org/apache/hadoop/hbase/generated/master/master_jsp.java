package org.apache.hadoop.hbase.generated.master;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.MetaRegion;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;

public final class master_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

  private static java.util.Vector _jspx_dependants;

  public java.util.List getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    JspFactory _jspxFactory = null;
    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;


    try {
      _jspxFactory = JspFactory.getDefaultFactory();
      response.setContentType("text/html;charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;


  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  HBaseConfiguration conf = master.getConfiguration();
  HServerAddress rootLocation = master.getRootRegionLocation();
  Map<byte [], MetaRegion> onlineRegions = master.getOnlineMetaRegions();
  Map<String, HServerInfo> serverToServerInfos =
    master.getServersToServerInfo();
  int interval = conf.getInt("hbase.regionserver.msginterval", 3000)/1000;
  if (interval == 0) {
      interval = 1;
  }

      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\r\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \r\n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \r\n<html xmlns=\"http://www.w3.org/1999/xhtml\">\r\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\r\n      <meta http-equiv=\"refresh\" content=\"300\"/>\r\n<title>HBase Master: ");
      out.print( master.getMasterAddress().getHostname());
      out.write(':');
      out.print( master.getMasterAddress().getPort() );
      out.write("</title>\r\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\r\n</head>\r\n\r\n<body>\r\n\r\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"HBase Logo\" title=\"HBase Logo\" /></a>\r\n<h1 id=\"page_title\">Master: ");
      out.print(master.getMasterAddress().getHostname());
      out.write(':');
      out.print(master.getMasterAddress().getPort());
      out.write("</h1>\r\n<p id=\"links_menu\"><a href=\"/logs/\">Local logs</a>, <a href=\"/stacks\">Thread Dump</a>, <a href=\"/logLevel\">Log Level</a></p>\r\n<hr id=\"head_rule\" />\r\n\r\n<h2>Master Attributes</h2>\r\n<table>\r\n<tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>\r\n<tr><td>HBase Version</td><td>");
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getVersion() );
      out.write(',');
      out.write(' ');
      out.write('r');
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getRevision() );
      out.write("</td><td>HBase version and svn revision</td></tr>\r\n<tr><td>HBase Compiled</td><td>");
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getDate() );
      out.write(',');
      out.write(' ');
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getUser() );
      out.write("</td><td>When HBase version was compiled and by whom</td></tr>\r\n<tr><td>Hadoop Version</td><td>");
      out.print( org.apache.hadoop.util.VersionInfo.getVersion() );
      out.write(',');
      out.write(' ');
      out.write('r');
      out.print( org.apache.hadoop.util.VersionInfo.getRevision() );
      out.write("</td><td>Hadoop version and svn revision</td></tr>\r\n<tr><td>Hadoop Compiled</td><td>");
      out.print( org.apache.hadoop.util.VersionInfo.getDate() );
      out.write(',');
      out.write(' ');
      out.print( org.apache.hadoop.util.VersionInfo.getUser() );
      out.write("</td><td>When Hadoop version was compiled and by whom</td></tr>\r\n<tr><td>HBase Root Directory</td><td>");
      out.print( master.getRootDir().toString() );
      out.write("</td><td>Location of HBase home directory</td></tr>\r\n<tr><td>Load average</td><td>");
      out.print( master.getAverageLoad() );
      out.write("</td><td>Average load across all region servers. Naive computation.</td></tr>\r\n<tr><td>Regions On FS</td><td>");
      out.print( master.countRegionsOnFS() );
      out.write("</td><td>The Number of regions on FileSystem. Rough count.</td></tr>\r\n</table>\r\n\r\n<h2>Catalog Tables</h2>\r\n");
 
  if (rootLocation != null) { 
      out.write("\r\n<table>\r\n<tr><th>Table</th><th>Description</th></tr>\r\n<tr><td><a href=/table.jsp?name=");
      out.print( Bytes.toString(HConstants.ROOT_TABLE_NAME) );
      out.write('>');
      out.print( Bytes.toString(HConstants.ROOT_TABLE_NAME) );
      out.write("</a></td><td>The -ROOT- table holds references to all .META. regions.</td></tr>\r\n");

    if (onlineRegions != null && onlineRegions.size() > 0) { 
      out.write("\r\n<tr><td><a href=/table.jsp?name=");
      out.print( Bytes.toString(HConstants.META_TABLE_NAME) );
      out.write('>');
      out.print( Bytes.toString(HConstants.META_TABLE_NAME) );
      out.write("</a></td><td>The .META. table holds references to all User Table regions</td></tr>\r\n  \r\n");
  } 
      out.write("\r\n</table>\r\n");
} 
      out.write("\r\n\r\n<h2>User Tables</h2>\r\n");
 HTableDescriptor[] tables = new HBaseAdmin(conf).listTables(); 
   if(tables != null && tables.length > 0) { 
      out.write("\r\n<table>\r\n<tr><th>Table</th><th>Description</th></tr>\r\n");
   for(HTableDescriptor htDesc : tables ) { 
      out.write("\r\n<tr><td><a href=/table.jsp?name=");
      out.print( htDesc.getNameAsString() );
      out.write('>');
      out.print( htDesc.getNameAsString() );
      out.write("</a> </td><td>");
      out.print( htDesc.toString() );
      out.write("</td></tr>\r\n");
   }  
      out.write("\r\n<p> ");
      out.print( tables.length );
      out.write(" table(s) in set.</p>\r\n</table>\r\n");
 } 
      out.write("\r\n\r\n<h2>Region Servers</h2>\r\n");
 if (serverToServerInfos != null && serverToServerInfos.size() > 0) { 
      out.write('\r');
      out.write('\n');
   int totalRegions = 0;
     int totalRequests = 0; 

      out.write("\r\n\r\n<table>\r\n<tr><th rowspan=");
      out.print( serverToServerInfos.size() + 1);
      out.write("></th><th>Address</th><th>Start Code</th><th>Load</th></tr>\r\n");
   String[] serverNames = serverToServerInfos.keySet().toArray(new String[serverToServerInfos.size()]);
     Arrays.sort(serverNames);
     for (String serverName: serverNames) {
       HServerInfo hsi = serverToServerInfos.get(serverName);
       String hostname = hsi.getServerAddress().getHostname() + ":" + hsi.getInfoPort();
       String url = "http://" + hostname + "/";
       totalRegions += hsi.getLoad().getNumberOfRegions();
       totalRequests += hsi.getLoad().getNumberOfRequests() / interval;
       long startCode = hsi.getStartCode();

      out.write("\r\n<tr><td><a href=\"");
      out.print( url );
      out.write('"');
      out.write('>');
      out.print( hostname );
      out.write("</a></td><td>");
      out.print( startCode );
      out.write("</td><td>");
      out.print( hsi.getLoad().toString(interval) );
      out.write("</td></tr>\r\n");
   } 
      out.write("\r\n<tr><th>Total: </th><td>servers: ");
      out.print( serverToServerInfos.size() );
      out.write("</td><td>&nbsp;</td><td>requests=");
      out.print( totalRequests );
      out.write(", regions=");
      out.print( totalRegions );
      out.write("</td></tr>\r\n</table>\r\n\r\n<p>Load is requests per second and count of regions loaded</p>\r\n");
 } 
      out.write("\r\n</body>\r\n</html>\r\n");
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      if (_jspxFactory != null) _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
