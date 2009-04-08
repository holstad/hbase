package org.apache.hadoop.hbase.generated.master;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.util.List;
import java.util.regex.*;
import org.apache.hadoop.hbase.RegionHistorian;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.RegionHistorian.RegionHistoryInformation;
import org.apache.hadoop.hbase.HConstants;

public final class regionhistorian_jsp extends org.apache.jasper.runtime.HttpJspBase
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


  String regionName = request.getParameter("regionname");
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  List<RegionHistoryInformation> informations = RegionHistorian.getInstance().getRegionHistory(regionName);
  // Pattern used so we can wrap a regionname in an href.
  Pattern pattern = Pattern.compile(RegionHistorian.SPLIT_PREFIX + "(.*)$");

      out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \n  \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"> \n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<head><meta http-equiv=\"Content-Type\" content=\"text/html;charset=UTF-8\"/>\n      <meta http-equiv=\"refresh\" content=\"30\"/>\n<title>Region in ");
      out.print( regionName );
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hbase.css\" />\n</head>\n\n<body>\n<a id=\"logo\" href=\"http://wiki.apache.org/lucene-hadoop/Hbase\"><img src=\"/static/hbase_logo_med.gif\" alt=\"HBase Logo\" title=\"HBase Logo\" /></a>\n<h1 id=\"page_title\">Region ");
      out.print( regionName );
      out.write("</h1>\n<p id=\"links_menu\"><a href=\"/master.jsp\">Master</a>, <a href=\"/logs/\">Local logs</a>, <a href=\"/stacks\">Thread Dump</a>, <a href=\"/logLevel\">Log Level</a></p>\n<hr id=\"head_rule\" />\n");
if(informations != null && informations.size() > 0) { 
      out.write("\n<table><tr><th>Timestamp</th><th>Event</th><th>Description</th></tr>\n");
  for( RegionHistoryInformation information : informations) {
    String description = information.getDescription();
    Matcher m = pattern.matcher(description);
    if (m.matches()) {
      // Wrap the region name in an href so user can click on it.
      description = RegionHistorian.SPLIT_PREFIX +
      "<a href=\"regionhistorian.jsp?regionname=" + m.group(1) + "\">" +
        m.group(1) + "</a>";
    }
    
    
      out.write("\n<tr><td>");
      out.print( information.getTimestampAsString() );
      out.write("</td><td>");
      out.print( information.getEvent() );
      out.write("</td><td>");
      out.print( description );
      out.write("</td></tr>\n");
  } 
      out.write("\n</table>\n<p>\nMaster is the source of following events: creation, open, and assignment.  Regions are the source of following events: split, compaction, and flush.\n</p>\n");
} else {
      out.write("\n<p>\nThis region is no longer available. It may be due to a split, a merge or the name changed.\n</p>\n");
} 
      out.write("\n\n\n</body>\n</html>\n");
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
