package org.apache.hadoop.hbase.generated.regionserver;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HRegionInfo;

public final class regionserver_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  HRegionServer regionServer = (HRegionServer)getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  HServerInfo serverInfo = regionServer.getServerInfo();
  RegionServerMetrics metrics = regionServer.getMetrics();
  Collection<HRegionInfo> onlineRegions = regionServer.getSortedOnlineRegionInfos();
  int interval = regionServer.getConfiguration().getInt("hbase.regionserver.msginterval", 3000)/1000;


      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n      <meta http-equiv=\"refresh\" content=\"300\"/>\n<title>HBase Region Server: ");
      out.print( serverInfo.getServerAddress().getHostname() );
      out.write(':');
      out.print( serverInfo.getServerAddress().getPort() );
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n\n<body>\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"HBase Logo\" title=\"HBase Logo\" /></a>\n<h1 id=\"page_title\">Region Server: ");
      out.print( serverInfo.getServerAddress().getHostname() );
      out.write(':');
      out.print( serverInfo.getServerAddress().getPort() );
      out.write("</h1>\n<p id=\"links_menu\"><a href=\"/logs/\">Local logs</a>, <a href=\"/stacks\">Thread Dump</a>, <a href=\"/logLevel\">Log Level</a></p>\n<hr id=\"head_rule\" />\n\n<h2>Region Server Attributes</h2>\n<table>\n<tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>\n<tr><td>HBase Version</td><td>");
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getVersion() );
      out.write(',');
      out.write(' ');
      out.write('r');
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getRevision() );
      out.write("</td><td>HBase version and svn revision</td></tr>\n<tr><td>HBase Compiled</td><td>");
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getDate() );
      out.write(',');
      out.write(' ');
      out.print( org.apache.hadoop.hbase.util.VersionInfo.getUser() );
      out.write("</td><td>When HBase version was compiled and by whom</td></tr>\n<tr><td>Metrics</td><td>");
      out.print( metrics.toString() );
      out.write("</td><td>RegionServer Metrics; file and heap sizes are in megabytes</td></tr>\n</table>\n\n<h2>Online Regions</h2>\n");
 if (onlineRegions != null && onlineRegions.size() > 0) { 
      out.write("\n<table>\n<tr><th>Region Name</th><th>Encoded Name</th><th>Start Key</th><th>End Key</th><th>Metrics</th></tr>\n");
   for (HRegionInfo r: onlineRegions) { 
        HServerLoad.RegionLoad load = regionServer.createRegionLoad(r.getRegionName());
 
      out.write("\n<tr><td>");
      out.print( r.getRegionNameAsString() );
      out.write("</td><td>");
      out.print( r.getEncodedName() );
      out.write("</td>\n    <td>");
      out.print( Bytes.toString(r.getStartKey()) );
      out.write("</td><td>");
      out.print( Bytes.toString(r.getEndKey()) );
      out.write("</td>\n    <td>");
      out.print( load.toString() );
      out.write("</td>\n    </tr>\n");
   } 
      out.write("\n</table>\n<p>Region names are made of the containing table's name, a comma,\nthe start key, a comma, and a randomly generated region id.  To illustrate,\nthe region named\n<em>domains,apache.org,5464829424211263407</em> is party to the table \n<em>domains</em>, has an id of <em>5464829424211263407</em> and the first key\nin the region is <em>apache.org</em>.  The <em>-ROOT-</em>\nand <em>.META.</em> 'tables' are internal sytem tables (or 'catalog' tables in db-speak).\nThe -ROOT- keeps a list of all regions in the .META. table.  The .META. table\nkeeps a list of all regions in the system. The empty key is used to denote\ntable start and table end.  A region with an empty start key is the first region in a table.\nIf region has both an empty start and an empty end key, its the only region in the table.  See\n<a href=\"http://hbase.org\">HBase Home</a> for further explication.<p>\n");
 } else { 
      out.write("\n<p>Not serving regions</p>\n");
 } 
      out.write("\n</body>\n</html>\n");
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
