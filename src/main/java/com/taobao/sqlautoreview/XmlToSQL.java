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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.dom4j.Comment;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;




/*
 * function:将xml中的sql解析出来,并保存到数据库中
 */
public class XmlToSQL {
	//log4j日志
	private static Logger logger = Logger.getLogger(XmlToSQL.class);
	//SQL MAP FILE id
	int sqlmap_file_id;
	//需要处理的SQL MAP FILE
	String sqlmapfilename;
	//保存引用
	HashMap<String,String> hash;
	//数据源操作
	IHandleDB wsdb ;
	//构造函数
	public XmlToSQL(IHandleDB iHandleSQLReviewDB,int sqlmap_file_id,String sqlmapfilename){
		 this.sqlmap_file_id = sqlmap_file_id;
		 this.sqlmapfilename = sqlmapfilename;
		 this.hash = new HashMap<String,String>();
		 this.wsdb=iHandleSQLReviewDB;
	}
	//构造函数
	public XmlToSQL()
	{
		HandleXMLConf sqlmapfileconf=new HandleXMLConf("sqlmapfile.xml");
		this.sqlmap_file_id=sqlmapfileconf.getSQLMapFileID();
		this.sqlmapfilename=sqlmapfileconf.getSQLMapFileName();
		this.hash = new HashMap<String,String>();
		this.wsdb=new HandleSQLReviewDB();
	}
	
	//正则表达式，美化SQL
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
	   
	   

	/**
	 * 加载XML文件成为文档对象
	 *  
	 */
	private Document loadXml(String path) throws DocumentException, FileNotFoundException {
		//注意这里使用的是FileInptStream，而不是FileReader
		InputStream input = new FileInputStream(path);
		SAXReader reader = new SAXReader();
		Document doc = reader.read(input);
		return doc;
	}
	
	
	/*
	 * 第二代xml解析器,得到真正的SQL语句
	 * 1.处理<![CDATA[<=]]>
	 * 2.处理include
	 * 3.处理prepend
	 */
	private String getRealSQL(Element sqlElement)
	{
		String sql_xml= sqlElement.asXML();
		//长度
		int sql_length=sql_xml.length();
		//最终返回的SQL
		String last_sql="";
		//>的地址
		int addr_right_kuohao;
		//prepend的地址
		int addr_prepend;
		//<>中的子字符串
		String sub_sql_xml;
		//引用的ID
		String refid;
		//跳越的步数
		int skip_step=0;
		for(int i=0;i<sql_length;i++)
		{
			if(skip_step>0)
			{
				skip_step--;
				continue;
			}
			if(sql_xml.substring(i,i+1).equals("<")==true)
			{
				addr_right_kuohao=sql_xml.indexOf(">", i);
				sub_sql_xml=sql_xml.substring(i, addr_right_kuohao);
				//这种情况需要重新计算右括号位置
				if(sub_sql_xml.indexOf("![CDATA[")>0)
				{
					addr_right_kuohao=sql_xml.indexOf(">", sql_xml.indexOf("]]", i));
					sub_sql_xml=sql_xml.substring(i, addr_right_kuohao);
				}
				//当前只处理如下的三种标签,其它标签全部过滤掉 
				if(sub_sql_xml.indexOf("include")>0 && sub_sql_xml.indexOf("refid")>0)
				{
					//include
					refid=sub_sql_xml.substring(sub_sql_xml.indexOf("\"")+1, sub_sql_xml.lastIndexOf("\"")).trim();
					String refsql=hash.get(refid);
					if(refsql==null){
						if(refid.indexOf(".")>0 && (hash.get(refid.substring(refid.indexOf(".")+1))) != null)
						{
							refsql=hash.get(refid.substring(refid.indexOf(".")+1));
							last_sql=last_sql+refsql;
							logger.warn("本条SQL出现了非常规引用,尝试处理成功.");
						}else {
							logger.error("本条SQL的引用无法处理.");
						}
					}else{
						last_sql=last_sql+refsql;
					}
				}
				else if(sub_sql_xml.indexOf("![CDATA[")>0)
				{
					//CDATA
					last_sql=last_sql+sub_sql_xml.substring(sub_sql_xml.indexOf("![CDATA[")+8, sub_sql_xml.indexOf("]]"));
				}
				else if(sub_sql_xml.indexOf("prepend")>0)
				{
					//prepend
					addr_prepend=sub_sql_xml.indexOf("prepend");
					int addr_first_yinhao=sub_sql_xml.indexOf("\"", addr_prepend);
					int addr_last_yinhao=sub_sql_xml.indexOf("\"", addr_first_yinhao+1);
					last_sql=last_sql+sub_sql_xml.substring(addr_first_yinhao+1, addr_last_yinhao);	
				}
				//计算跳越的步长
				skip_step=sub_sql_xml.length();
			}
			else {
				last_sql=last_sql+sql_xml.substring(i, i+1);
			}
		}//end for
		
		return last_sql;
	}
	
	/*
	 * 对于引用的SQL,需要follow如下的xml规范
	 * <sql id=>
	 * 把所有可能出现引用的SQL统一保存到一个hashmap里
	 */
	@SuppressWarnings("unchecked")
	private void dealInclude(Element root)
	{
		String refid;
		String refsql;
		Element sqlElement;
		for( Iterator<Element> iterator = root.elementIterator();iterator.hasNext();)
		{
		    sqlElement=iterator.next();
		    if(sqlElement.getName().equals("sql") && sqlElement.attributeValue("id") !=null)
		    {
		    	refid=sqlElement.attributeValue("id");
				refsql=getRealSQL(sqlElement);
				hash.put(refid, refsql);
		    }
		    else {
				continue;
			}
		}
	}
	
	/*
	 * 取得下一个单词,遇到单词后的空格后停止
	 */
    private String getNextToken(String str,int from_addr)
    {
    	String token="";
    	//参数安全检查
    	if(str==null || str.length()<from_addr){
    		return null;
    	}
    	//空格
    	while(from_addr<str.length() && str.substring(from_addr, from_addr+1).equals(" "))
    	{
    		from_addr++;
    	}
    	//检查退出条件
    	if(from_addr>str.length()){
    		return null;
    	}
    	//token
    	while(from_addr<str.length() && str.substring(from_addr, from_addr+1).equals(" ")==false)
    	{
    		token=token+str.substring(from_addr, from_addr+1);
    		from_addr++;
    	}
    	
    	return token;
    }
    
	/*
	 * 对SQL进行预处理,对常见问题,进行过滤
	 */
	private String preDealSql(String sqlString) 
	{
		int addr_where;
		int addr_first_and;
		if(sqlString==null) return null;
		sqlString=sqlString.toLowerCase();
		//将where后的第一个and删除
		addr_where=sqlString.indexOf(" where ");
		if(addr_where>0 && addr_where+7<sqlString.length()){
			if(getNextToken(sqlString, addr_where+7).equals("and"))
			{
				//where后第一个词出现了and
				addr_first_and=sqlString.indexOf("and", addr_where+7);
				sqlString=sqlString.substring(0, addr_first_and)+sqlString.substring(addr_first_and+3);
			}
		}
		
		sqlString=sqlString.replace("&gt;", ">");
		sqlString=sqlString.replace("&lt;", "<");
		return sqlString;
	}
	/**
	 * 读SQL MAP文件,解析出SQL语句
	 *  	
	 * @throws DocumentException
	 */
	private void readSqlMap() throws DocumentException {
		Element root;
		if(sqlmapfilename==null) return;     
		try{
            Document dom = loadXml(sqlmapfilename);
            root = dom.getRootElement();
            if(root == null){ 
            	logger.error("can not find sql map file xml root node,sqlautoreview program exit.");
            	return;
            }
		}
        catch(FileNotFoundException e)
        {
        	logger.error("the sql-map-file don't exist,please check the path.");
        	return;
        }
            
            //先保存一下引用的 SQL
            dealInclude(root);
            //Element结点
    		Element sqlElement = null;
    		//循环控制
    		int max_loop_count=0; 
    		//保存处理过后的SQL
    		String real_sql; 
    		//保存开发编写的SQL注解
    		String commentString=""; 
    		//持久化到数据库的对象

    		for (int i=0; i < root.nodeCount(); i++)
    		{
    			//循环次数控制,超过100000次,强制退出循环
    			max_loop_count++;
    		    if(max_loop_count>100000)
    		    {
    		    	logger.error("the sql map file has more than 100000 sqls.Sql reveiw exit. Please check the sql map file.");
    		    	break;
    		    }
    			Node node=root.node(i);
    			if(node instanceof Element)
    			{
    		        sqlElement = (Element) node;
    			}
    			else {
					continue;
				}
    			  	
    		    //只需要处理四种SQL:select,update,delete,insert
    		    if(sqlElement.getName().equals("select") || sqlElement.getName().equals("update") ||
    		    	sqlElement.getName().equals("delete") || sqlElement.getName().equals("insert"))
    		    {
    		    	real_sql=formatSql(getRealSQL(sqlElement));
    		    	//对SQL进行预处理
    		    	real_sql=preDealSql(real_sql);
    		    	logger.info("java class id="+sqlElement.attributeValue("id")+" sql="+real_sql);
                    //处理一种特殊字符,字段值里可能含有单引号
                    real_sql=real_sql.replace("'", "''");
                    //处理sql_xml
                    String sql_xml= sqlElement.asXML();
                    sql_xml=sql_xml.replace("'", "''");
                    //处理SQL MAP中的comment
                    if(root.node(i-1)!=null && root.node(i-1) instanceof Comment)
                    {
                    	commentString=root.node(i-1).asXML();
                    	commentString=commentString.replace("'", "''");
                    }
                    //持久化到数据库中
                    wsdb.insertDB(sqlmap_file_id,sqlElement.attributeValue("id"), sql_xml, real_sql,commentString);		
    		    }
    		    commentString="";  
    		}
	}
	
	/*
	 * 前端调用的方法
	 */
	public void xmlToSqlService() throws DocumentException, IOException 
	{
		    readSqlMap();
	}
	
	/**
	 * @param args
	 * @throws DocumentException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws DocumentException, IOException 
	{
		
		    XmlToSQL xts = new XmlToSQL();
		    if(xts.wsdb.checkConnection()==false){
		    	logger.error("无法连接上sqlreviewdb,请检查配置文件.");
		    	return;
		    }
		    logger.info("SQLMAP FILE解析文件开始");
		    xts.readSqlMap();
		    logger.info("SQLMAP FILE解析文件结束");
	}
}
