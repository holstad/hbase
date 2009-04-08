package org.apache.hadoop.hbase.generated.master;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetaRegion;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hbase.HConstants;

public final class table_jsp extends org.apache.jasper.runtime.HttpJspBase
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
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  String tableName = request.getParameter("name");
  HTable table = new HTable(conf, tableName);
  Map<HServerAddress, HServerInfo> serverAddressToServerInfos =
      master.getServerAddressToServerInfo();
  String tableHeader = "<h2>Table Regions</h2><table><tr><th>Name</th><th>Region Server</th><th>Encoded Name</th><th>Start Key</th><th>End Key</th></tr>";
  HServerAddress rootLocation = master.getRootRegionLocation();

      out.write("\n\n<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n\n");

  String action = request.getParameter("action");
  String key = request.getParameter("key");
  if ( action != null ) {

      out.write("\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n      <meta http-equiv=\"refresh\" content=\"5; url=/\"/>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n<body>\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"HBase Logo\" title=\"HBase Logo\" /></a>\n<h1 id=\"page_title\">Table action request accepted</h1>\n<p><hr><p>\n");

  if (action.equals("split")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_SPLIT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_SPLIT, null);
    }
    
      out.write(" Split request accepted. ");

  } else if (action.equals("compact")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_COMPACT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_COMPACT, null);
    }
    
      out.write(" Compact request accepted. ");

  }

      out.write("\n<p>This page will refresh in 5 seconds.\n</body>\n");

} else {

      out.write("\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n      <meta http-equiv=\"refresh\" content=\"30\"/>\n<title>Table: ");
      out.print( tableName );
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n<body>\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"HBase Logo\" title=\"HBase Logo\" /></a>\n<h1 id=\"page_title\">Table: ");
      out.print( tableName );
      out.write("</h1>\n<p id=\"links_menu\"><a href=\"/master.jsp\">Master</a>, <a href=\"/logs/\">Local logs</a>, <a href=\"/stacks\">Thread Dump</a>, <a href=\"/logLevel\">Log Level</a></p>\n<hr id=\"head_rule\" />\n");

  if(tableName.equals(Bytes.toString(HConstants.ROOT_TABLE_NAME))) {

      out.write('\n');
      out.print( tableHeader );
      out.write('\n');

  int infoPort = serverAddressToServerInfos.get(rootLocation).getInfoPort();
  String url = "http://" + rootLocation.getHostname() + ":" + infoPort + "/";

      out.write("\n<tr>\n  <td>");
      out.print( tableName );
      out.write("</td>\n  <td><a href=\"");
      out.print( url );
      out.write('"');
      out.write('>');
      out.print( rootLocation.getHostname() );
      out.write(':');
      out.print( rootLocation.getPort() );
      out.write("</a></td>\n  <td>-</td>\n  <td></td>\n  <td>-</td>\n</tr>\n</table>\n");

  } else if(tableName.equals(Bytes.toString(HConstants.META_TABLE_NAME))) {

      out.write('\n');
      out.print( tableHeader );
      out.write('\n');

  Map<byte [], MetaRegion> onlineRegions = master.getOnlineMetaRegions();
  for (MetaRegion meta: onlineRegions.values()) {
    int infoPort = serverAddressToServerInfos.get(meta.getServer()).getInfoPort();
    String url = "http://" + meta.getServer().getHostname() + ":" + infoPort + "/";

      out.write("\n<tr>\n  <td>");
      out.print( Bytes.toString(meta.getRegionName()) );
      out.write("</td>\n    <td><a href=\"");
      out.print( url );
      out.write('"');
      out.write('>');
      out.print( meta.getServer().toString() );
      out.write("</a></td>\n    <td>-</td><td>");
      out.print( Bytes.toString(meta.getStartKey()) );
      out.write("</td><td>-</td>\n</tr>\n");
  } 
      out.write("\n</table>\n");
} else {
  try { 
      out.write("\n<h2>Table Attributes</h2>\n<table>\n  <tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>\n  <tr><td>Enabled</td><td>");
      out.print( hbadmin.isTableEnabled(table.getTableName()) );
      out.write("</td><td>Is the table enabled</td></tr>\n</table>\n");

  Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
  if(regions != null && regions.size() > 0) { 
      out.write('\n');
      out.print(     tableHeader );
      out.write('\n');

  for(Map.Entry<HRegionInfo, HServerAddress> hriEntry : regions.entrySet()) {
    
    int infoPort = serverAddressToServerInfos.get(
        hriEntry.getValue()).getInfoPort();
    
    String urlRegionHistorian =
        "/regionhistorian.jsp?regionname="+hriEntry.getKey().getRegionNameAsString();

    String urlRegionServer =
        "http://" + hriEntry.getValue().getHostname().toString() + ":" + infoPort + "/";

      out.write("\n<tr>\n  <td><a href=\"");
      out.print( urlRegionHistorian );
      out.write('"');
      out.write('>');
      out.print( hriEntry.getKey().getRegionNameAsString());
      out.write("</a></td>\n  <td><a href=\"");
      out.print( urlRegionServer );
      out.write('"');
      out.write('>');
      out.print( hriEntry.getValue().toString() );
      out.write("</a></td>\n  <td>");
      out.print( hriEntry.getKey().getEncodedName());
      out.write("</td> <td>");
      out.print( Bytes.toString(hriEntry.getKey().getStartKey()));
      out.write("</td>\n  <td>");
      out.print( Bytes.toString(hriEntry.getKey().getEndKey()));
      out.write("</td>\n</tr>\n");
 } 
      out.write("\n</table>\n");
 }
} catch(Exception ex) {
  ex.printStackTrace();
}
} // end else

      out.write("\n\n<p><hr><p>\nActions:\n<p>\n<center>\n<table style=\"border-style: none\" width=\"90%\">\n<tr>\n  <form method=\"get\">\n  <input type=\"hidden\" name=\"action\" value=\"compact\">\n  <input type=\"hidden\" name=\"name\" value=\"");
      out.print( tableName );
      out.write("\">\n  <td style=\"border-style: none; text-align: center\">\n      <input style=\"font-size: 12pt; width: 10em\" type=\"submit\" value=\"Compact\"></td>\n  <td style=\"border-style: none\" width=\"5%\">&nbsp;</td>\n  <td style=\"border-style: none\">Region Key (optional):<input type=\"text\" name=\"key\" size=\"40\"></td>\n  <td style=\"border-style: none\">This action will force a compaction of all\n  regions of the table, or, if a key is supplied, only the region containing the\n  given key.</td>\n  </form>\n</tr>\n<tr><td style=\"border-style: none\" colspan=\"4\">&nbsp;</td></tr>\n<tr>\n  <form method=\"get\">\n  <input type=\"hidden\" name=\"action\" value=\"split\">\n  <input type=\"hidden\" name=\"name\" value=\"");
      out.print( tableName );
      out.write("\">\n  <td style=\"border-style: none; text-align: center\">\n      <input style=\"font-size: 12pt; width: 10em\" type=\"submit\" value=\"Split\"></td>\n  <td style=\"border-style: none\" width=\"5%\">&nbsp;</td>\n  <td style=\"border-style: none\">Region Key (optional):<input type=\"text\" name=\"key\" size=\"40\"></td>\n  <td style=\"border-style: none\">This action will force a split of all eligible\n  regions of the table, or, if a key is supplied, only the region containing the\n  given key. An eligible region is one that does not contain any references to\n  other regions. Split requests for noneligible regions will be ignored.</td>\n  </form>\n</tr>\n</table>\n</center>\n<p>\n\n");

}

      out.write("\n\n</body>\n</html>\n");
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
