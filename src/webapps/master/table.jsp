<%@ page contentType="text/html;charset=UTF-8"
  import="java.io.IOException"
  import="java.util.Map"
  import="java.net.URLEncoder" 
  import="org.apache.hadoop.io.Text"
  import="org.apache.hadoop.io.Writable"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.client.HTable"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.master.HMaster" 
  import="org.apache.hadoop.hbase.master.MetaRegion"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.HConstants"%><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  HBaseConfiguration conf = master.getConfiguration();
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  String tableName = request.getParameter("name");
  HTable table = new HTable(conf, tableName);
  Map<HServerAddress, HServerInfo> serverAddressToServerInfos =
      master.getServerAddressToServerInfo();
  String tableHeader = "<h2>Table Regions</h2><table><tr><th>Name</th><th>Region Server</th><th>Encoded Name</th><th>Start Key</th><th>End Key</th></tr>";
  HServerAddress rootLocation = master.getRootRegionLocation();
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">

<%
  String action = request.getParameter("action");
  String key = request.getParameter("key");
  if ( action != null ) {
%>
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
      <meta http-equiv="refresh" content="5; url=/"/>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Table action request accepted</h1>
<p><hr><p>
<%
  if (action.equals("split")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_SPLIT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_SPLIT, null);
    }
    %> Split request accepted. <%
  } else if (action.equals("compact")) {
    if (key != null && key.length() > 0) {
      Writable[] arr = new Writable[1];
      arr[0] = new ImmutableBytesWritable(Bytes.toBytes(key));
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_COMPACT, arr);
    } else {
      master.modifyTable(Bytes.toBytes(tableName), HConstants.MODIFY_TABLE_COMPACT, null);
    }
    %> Compact request accepted. <%
  }
%>
<p>This page will refresh in 5 seconds.
</body>
<%
} else {
%>
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
      <meta http-equiv="refresh" content="30"/>
<title>Table: <%= tableName %></title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">Table: <%= tableName %></h1>
<p id="links_menu"><a href="/master.jsp">Master</a>, <a href="/logs/">Local logs</a>, <a href="/stacks">Thread Dump</a>, <a href="/logLevel">Log Level</a></p>
<hr id="head_rule" />
<%
  if(tableName.equals(Bytes.toString(HConstants.ROOT_TABLE_NAME))) {
%>
<%= tableHeader %>
<%
  int infoPort = serverAddressToServerInfos.get(rootLocation).getInfoPort();
  String url = "http://" + rootLocation.getHostname() + ":" + infoPort + "/";
%>
<tr>
  <td><%= tableName %></td>
  <td><a href="<%= url %>"><%= rootLocation.getHostname() %>:<%= rootLocation.getPort() %></a></td>
  <td>-</td>
  <td></td>
  <td>-</td>
</tr>
</table>
<%
  } else if(tableName.equals(Bytes.toString(HConstants.META_TABLE_NAME))) {
%>
<%= tableHeader %>
<%
  Map<byte [], MetaRegion> onlineRegions = master.getOnlineMetaRegions();
  for (MetaRegion meta: onlineRegions.values()) {
    int infoPort = serverAddressToServerInfos.get(meta.getServer()).getInfoPort();
    String url = "http://" + meta.getServer().getHostname() + ":" + infoPort + "/";
%>
<tr>
  <td><%= Bytes.toString(meta.getRegionName()) %></td>
    <td><a href="<%= url %>"><%= meta.getServer().toString() %></a></td>
    <td>-</td><td><%= Bytes.toString(meta.getStartKey()) %></td><td>-</td>
</tr>
<%  } %>
</table>
<%} else {
  try { %>
<h2>Table Attributes</h2>
<table>
  <tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>
  <tr><td>Enabled</td><td><%= hbadmin.isTableEnabled(table.getTableName()) %></td><td>Is the table enabled</td></tr>
</table>
<%
  Map<HRegionInfo, HServerAddress> regions = table.getRegionsInfo();
  if(regions != null && regions.size() > 0) { %>
<%=     tableHeader %>
<%
  for(Map.Entry<HRegionInfo, HServerAddress> hriEntry : regions.entrySet()) {
    
    int infoPort = serverAddressToServerInfos.get(
        hriEntry.getValue()).getInfoPort();
    
    String urlRegionHistorian =
        "/regionhistorian.jsp?regionname=" + 
        URLEncoder.encode(hriEntry.getKey().getRegionNameAsString(), "UTF-8");

    String urlRegionServer =
        "http://" + hriEntry.getValue().getHostname().toString() + ":" + infoPort + "/";
%>
<tr>
  <td><a href="<%= urlRegionHistorian %>"><%= hriEntry.getKey().getRegionNameAsString()%></a></td>
  <td><a href="<%= urlRegionServer %>"><%= hriEntry.getValue().toString() %></a></td>
  <td><%= hriEntry.getKey().getEncodedName()%></td> <td><%= Bytes.toString(hriEntry.getKey().getStartKey())%></td>
  <td><%= Bytes.toString(hriEntry.getKey().getEndKey())%></td>
</tr>
<% } %>
</table>
<% }
} catch(Exception ex) {
  ex.printStackTrace();
}
} // end else
%>

<p><hr><p>
Actions:
<p>
<center>
<table style="border-style: none" width="90%">
<tr>
  <form method="get">
  <input type="hidden" name="action" value="compact">
  <input type="hidden" name="name" value="<%= tableName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Compact"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a compaction of all
  regions of the table, or, if a key is supplied, only the region containing the
  given key.</td>
  </form>
</tr>
<tr><td style="border-style: none" colspan="4">&nbsp;</td></tr>
<tr>
  <form method="get">
  <input type="hidden" name="action" value="split">
  <input type="hidden" name="name" value="<%= tableName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Split"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a split of all eligible
  regions of the table, or, if a key is supplied, only the region containing the
  given key. An eligible region is one that does not contain any references to
  other regions. Split requests for noneligible regions will be ignored.</td>
  </form>
</tr>
</table>
</center>
<p>

<%
}
%>

</body>
</html>
