/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 *
 * Authors:
 *   danchen <danchen@taobao.com>
 *
 */

package com.taobao.sqlautoreview;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;



/*
 * sqlreviewdb 数据库读写操作
 */
public class HandleSQLReviewDB implements IHandleDB{
	    //log4j日志
	    private static Logger logger = Logger.getLogger(HandleSQLReviewDB.class);
	    
		public String IP;
		public int port;
		public String dbname;
		public String user;
		public String password;
		public Connection conn;
		
       //构造函数
       public HandleSQLReviewDB()
       {
    	   HandleXMLConf dbconfig = new HandleXMLConf("sqlreviewdb.xml");
    	   this.IP=dbconfig.getDbConfigIP();
    	   this.port=dbconfig.getDbConfigPort();
    	   this.dbname=dbconfig.getDbConfigDbname();
    	   this.user=dbconfig.getDbConfigUser();
    	   this.password=dbconfig.getDbConfigPassword();
    	   this.conn=getConnection();
       }
       
       //获得数据库连接
       public Connection getConnection()
       {
    	   String JDriver = "com.mysql.jdbc.Driver";
    	   String conURL="jdbc:mysql://"+IP+":"+port+"/"+dbname;
    	   try {
               Class.forName(JDriver);
           }
           catch(ClassNotFoundException cnf_e) {  
        	   // 如果找不到驱动类
        	   logger.error("Driver Not Found: ", cnf_e);
           }

           try {
               Connection conn = DriverManager.getConnection(conURL, this.user, this.password);  
               return conn;
           }
           catch(SQLException se)
           {
        	   logger.error("无法创建到数据库的连接.", se);
        	   return null;
           } 
       }
       
       //检查连接的有效性
	   	public boolean checkConnection() 
       {
	   		if(this.conn==null){
	   			return false;
	   		}else {
	   			return true;
			}	
	   	}
       
	     //这里的代码与XmlToSQL中的代码重复，主要是因为可能SQL有可能不通过XmlToSQL类进入数据库中
	   	 //为了避免出错，在parse每一条SQL之前,再次对每条SQL格式化一下
	   	public static String delByPattern(String str) {
	   	      Pattern p = Pattern.compile(" {2,}");
	   	      Matcher m = p.matcher(str);
	   	      String result = m.replaceAll(" ");
	   	      return result;
	   	   }
	   	
	   	public static String formatSql(String unFormatSql) {
	   	      String newSql = unFormatSql.trim().replace("\n", " ")
	   	            .replace("\t", " ").replace("\r", " ").replace("\f", " ");
	   	    
	   	      return delByPattern(newSql);
	   	   }
       //写SQL数据到数据库中
       public boolean insertDB(int sqlmap_file_id,String java_class_id,String sql_xml,String real_sql,String sql_comment)
       {
    	   try{
    		   sql_comment=new String(sql_comment.getBytes(),"GBK");
    	       String command="insert into xmltosql(sqlmap_file_id,java_class_id,sql_xml,real_sql,sql_comment,gmt_create,gmt_modified,status) "; 
    	       command=command+"values("+sqlmap_file_id+",'"+java_class_id+"','"+sql_xml+"','"+real_sql+"','"+sql_comment+"',"+"now(),"+"now(),"+"0);";
    	       Statement stmt = conn.createStatement();
    	       stmt.execute(command);
               stmt.close();
    	   }
    	   catch(Exception e)
    	   {
    		   logger.error("写入数据库出错", e);
    		   return false;
    	   }
     
    	   return true;
       }
       
       //从数据库里获得待审核的SQL
       public List<SQL_Node> getAllSQL()
       {
    	 
    	List<SQL_Node> list_sql = new LinkedList<SQL_Node>();
       	try
       	{
       	   HandleXMLConf sqlmapfileconf=new HandleXMLConf("sqlmapfile.xml");
    	   int sqlmap_file_id=sqlmapfileconf.getSQLMapFileID();
       	   String command="select id,real_sql from xmltosql where status = 0 and sqlmap_file_id="+sqlmap_file_id;
       	   Statement stmt = conn.createStatement();
   	       stmt.execute(command);
   	       ResultSet rs = stmt.getResultSet();
   	  
   	       while(rs.next())
   	       {
   	    	   SQL_Node snNode= new SQL_Node();
   	    	   snNode.id=rs.getInt("id");
   	    	   snNode.sqlString=rs.getString("real_sql");
   	    	   snNode.sqlString=formatSql(snNode.sqlString);
   	    	   list_sql.add(snNode);
   	       }
   	       
   	       rs.close();
           stmt.close();
           return list_sql;
       	}
           catch(SQLException e)
           {
        	   logger.error("检索待审核的SQL出错", e);
        	   return null;
           }
       }
       
     //从数据库里获得待审核的SQL
       public List<SQL_Node> getAllSQL(int sqlmapfileID)
       {
    	 
    	List<SQL_Node> list_sql = new LinkedList<SQL_Node>();
       	try
       	{
    	   int sqlmap_file_id=sqlmapfileID;
       	   String command="select id,real_sql from xmltosql where status = 0 and sqlmap_file_id="+sqlmap_file_id;
       	   Statement stmt = conn.createStatement();
   	       stmt.execute(command);
   	       ResultSet rs = stmt.getResultSet();
   	  
   	       while(rs.next())
   	       {
   	    	   SQL_Node snNode= new SQL_Node();
   	    	   snNode.id=rs.getInt("id");
   	    	   snNode.sqlString=rs.getString("real_sql");
   	    	   snNode.sqlString=formatSql(snNode.sqlString);
   	    	   list_sql.add(snNode);
   	       }
   	       
   	       rs.close();
           stmt.close();
           return list_sql;
       	}
           catch(SQLException e)
           {
        	   logger.error("检索待审核的SQL出错", e);
        	   return null;
           }
       }
       
       //更改单个SQL的审核状态,以及SQL结果
       public int updateSQLStatus(int status,String sql_auto_index,int id,String auto_review_err,String auto_review_tip)
       {
    	   String command="update xmltosql set status="+status+",sql_auto_index='"+sql_auto_index+"',";
    	   command=command+"auto_review_err='"+auto_review_err+"',";
    	   command=command+"auto_review_tip='"+auto_review_tip+"',";
		   command=command+"auto_review_time=now(),gmt_modified=now() where id="+id+";";
    	   try {
				Statement stmt = conn.createStatement();
		   	    stmt.execute(command);
		   	    if(stmt.getUpdateCount()!=1)
		   	    	return -1;
		   	    else {
					return 0;
				}
			} catch (SQLException e) {
				logger.error("执行command="+command+"出现异常", e);
				return -2;
			}
       }
       
       //更改单个SQL的审核状态,以及SQL结果
       public int updateSQLStatus(int status,String sql_auto_index,int id,String auto_review_err,String auto_review_tip,String tablenames)
       {
    	   String command="update xmltosql set status="+status+",sql_auto_index='"+sql_auto_index+"',";
    	   command=command+"auto_review_err='"+auto_review_err+"',";
    	   command=command+"auto_review_tip='"+auto_review_tip+"',";
    	   command=command+"table_name='"+tablenames+"',";
		   command=command+"auto_review_time=now(),gmt_modified=now() where id="+id+";";
    	   try {
				Statement stmt = conn.createStatement();
		   	    stmt.execute(command);
		   	    if(stmt.getUpdateCount()!=1)
		   	    	return -1;
		   	    else {
					return 0;
				}
			} catch (SQLException e) {
				logger.error("执行command="+command+"出现异常", e);
				return -2;
			}
       }

	@Override
	public List<Index_Node> getAllIndexes() 
	{
		List<Index_Node> list_indexes = new LinkedList<Index_Node>();
       	try
       	{
       	   HandleXMLConf sqlmapfileconf=new HandleXMLConf("sqlmapfile.xml");
    	   int sqlmap_file_id=sqlmapfileconf.getSQLMapFileID();
       	   String command="select table_name,sql_auto_index from xmltosql where status = 1 and sqlmap_file_id="+sqlmap_file_id;
       	   Statement stmt = conn.createStatement();
   	       stmt.execute(command);
   	       ResultSet rs = stmt.getResultSet();
   	  
   	       while(rs.next())
   	       {
   	    	   Index_Node index_Node=new Index_Node();
   	    	   //这条SQL所涉及的表名
   	    	   index_Node.table_name=rs.getString("table_name");
   	    	   //这条SQL创建索引的脚本
   	    	   index_Node.index_name=rs.getString("sql_auto_index");
   	    	   list_indexes.add(index_Node);
   	       }
   	       
   	       rs.close();
           stmt.close();
           return list_indexes;
       	}
           catch(SQLException e)
           {
        	   logger.error("检索已生成的indexes出错", e);
        	   return null;
           }
	}

	/*
	 * 自动化测试使用
	 */
	public void deleteSqlByID(int sqlmap_file_id)
	{
		String command="delete from xmltosql where sqlmap_file_id="+sqlmap_file_id;
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute(command);
			int rows=stmt.getUpdateCount();
			stmt.close();
			logger.info(rows+" is deleted.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 自动化测试使用
	 */
	public void closeConn() {
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Override
	public List<Index_Node> getAllIndexes(int sqlmap_file_id) 
	{
		List<Index_Node> list_indexes = new LinkedList<Index_Node>();
       	try
       	{
       	   String command="select table_name,sql_auto_index from xmltosql where status = 1 and sqlmap_file_id="+sqlmap_file_id;
       	   Statement stmt = conn.createStatement();
   	       stmt.execute(command);
   	       ResultSet rs = stmt.getResultSet();
   	  
   	       while(rs.next())
   	       {
   	    	   Index_Node index_Node=new Index_Node();
	    	   //这条SQL所涉及的表名,可能有多个表名,用,分隔
	    	   index_Node.table_name=rs.getString("table_name");
	    	   //这条SQL创建索引的脚本,可能有多条建索引的语句,以;分隔
	    	   index_Node.index_name=rs.getString("sql_auto_index");
	    	   list_indexes.add(index_Node);
   	       }
   	       
   	       rs.close();
           stmt.close();
           return list_indexes;
       	}
           catch(SQLException e)
           {
        	   logger.error("检索已生成的indexes出错", e);
        	   return null;
           }
	}

	
	/*
	 * 删除merge结果
	 */
	@Override
	public void deleteMergeResult(int sqlmap_file_id) {
		
		String command="delete from mergeresult where sqlmap_file_id="+sqlmap_file_id;
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute(command);
			int rows=stmt.getUpdateCount();
			stmt.close();
			logger.debug("mergeresult sqlmap_file_id "+sqlmap_file_id+rows+" is deleted.");
		} catch (SQLException e) {
			logger.error("some error happen:",e);
		}
	}

	/*
	 * 插入merge结果
	 */
	@Override
	public void saveMergeResult(int sqlmap_file_id, String tablename,
			String real_tablename, String exist_indexes, String new_indexes,
			String merge_result) {
		String command="insert into mergeresult(sqlmap_file_id,tablename,real_tablename,exist_indexes,new_indexes,merge_result,gmt_create,gmt_modified) values(";
		command=command+sqlmap_file_id+",'"+tablename+"','"+real_tablename+"','"+exist_indexes+"','"+new_indexes+"','"+merge_result+"',now(),now())";
		logger.debug(command);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.execute(command);
			stmt.close();
		} catch (SQLException e) {
			logger.error("some error happen:",e);
		}
		
	}
}
