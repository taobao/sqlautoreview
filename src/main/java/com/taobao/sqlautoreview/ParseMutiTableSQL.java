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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
/*
 * 处理多表查询的select类
 */
public class ParseMutiTableSQL extends ParseSQL {
	//log4j日志
	private static Logger logger = Logger.getLogger(ParseMutiTableSQL.class);
	//保存每个表的分析结果
	public List<ParseStruct> list_ParseStruct;
	//保存表与表的join关系
	public List<Table_Relationship> list_Table_Relationship ;
	
	
	public ParseMutiTableSQL(String sql) {
		super(sql);
		//多表标志
		tag=1;
		//保存各表的信息
		list_ParseStruct = new LinkedList<ParseStruct>();
		//保存表与表的关系
		list_Table_Relationship = new LinkedList<Table_Relationship>();
	}
	
	//分析from后的table
	public int AnalyzeMutipleTable(String fromString) 
	{
		logger.debug("enter function AnalyzeMutipleTable");
		int length=fromString.trim().length();
		int i=0;
		int start=i;
		while(i<=length)
		{
				ParseStruct parsestruct = new ParseStruct();
				//table name
				while(i+1<=length && fromString.substring(i, i+1).equals(" ")==false)
					i++;
				parsestruct.tablename=fromString.substring(start, i).trim();
				logger.debug("table name:"+parsestruct.tablename);
				//space
				while(i+1<=length && fromString.substring(i, i+1).equals(" ")==true)
					i++;
				//别名检查
				if(i+1<=length && fromString.substring(i, i+1).equals(",")==true)
				{
					errmsg="table has no alias table name.";
					logger.warn(errmsg);
					return -1;
				}
				
				//检查有没有tablename as t1这种特殊语法
				if(i+3<=fromString.length() && fromString.substring(i, i+3).equals("as ")==true){
					i=i+3;
				}
				//alias table name
				start=i;
				while(i+1<=length && fromString.substring(i,i+1).equals(",")==false)
					i++;
				parsestruct.alias_tablename=fromString.substring(start, i).trim();
				logger.debug("alias table name:"+parsestruct.alias_tablename);
				//将这个表,别名添加进链表里
				list_ParseStruct.add(parsestruct);
				if(i+1>length)
				{
					break;
				}
				//跳过, and space
				i++;
				while(i+1<=length && fromString.substring(i, i+1).equals(" ")==true)
					i++;
				start=i;
		}
		
		return 0;
	}
    
	//分析whereString,对不同表的条件进行归类存储,列名前必须要加上别名
	//驱动表的连接键不能作为条件;被驱动表的连接键可以作为条件,而且还是前导列
	public int AnalyzeWhereStr(String whereString)
	{
		logger.debug("enter function AnalyzeWhereStr");
		logger.debug("whereString:"+whereString);
		//先作一个检查
		if(whereString.indexOf(" between ") > 0){
			whereString=handleBetweenAnd(whereString);
		}
		logger.debug("whereString:"+whereString);
		//将条件生成一颗树
		Tree_Node rootnode=parseWhere(null,whereString,0);
		//遍历这棵树，将树中的条件放到对应的表上
		for(Iterator<ParseStruct> r=list_ParseStruct.iterator();r.hasNext();)
		{
			ParseStruct tmp_ps=r.next();
			fillwhereString(tmp_ps,rootnode);
			tmp_ps.whereString=tmp_ps.whereString.substring(0, tmp_ps.whereString.length()-1);
			tmp_ps.whereString=tmp_ps.whereString.replace(tmp_ps.alias_tablename+".", "");
			//log
			logger.debug("table name:"+tmp_ps.tablename);
			logger.debug("alias table name:"+tmp_ps.alias_tablename);
			logger.debug("whereString:"+tmp_ps.whereString);
		}
		
		findFirstAndLastTable();
		findOtherTable(list_Table_Relationship.get(0), list_Table_Relationship.get(1), rootnode,rootnode);
		logger.debug("tag:"+list_Table_Relationship.size());
		logger.debug("join table order:");
		for(int i=0;i<list_Table_Relationship.size();i++)
		{
			Table_Relationship tRelationship=list_Table_Relationship.get(i);
			logger.debug("tablename:"+tRelationship.tablename+" alias tablename:"+tRelationship.alias_tablename);
			logger.debug("join column1:"+tRelationship.columnname1+" join column2:"+tRelationship.columnname2);
		}
		
		return 0;
	}
	
	/*
	 * 找关联表的第一个表,和最后一个表
	 */
	private boolean findFirstAndLastTable()
	{
		boolean is_find=true;
		boolean is_find_head=false;
		for(Iterator<ParseStruct> r=list_ParseStruct.iterator();r.hasNext();)
		{
			ParseStruct tmp_ps=r.next();
			if(tmp_ps.whereString.indexOf(":1")==tmp_ps.whereString.lastIndexOf(":1"))
			{
				Table_Relationship table_rel_node = new Table_Relationship();
				table_rel_node.tablename=tmp_ps.tablename;
				table_rel_node.alias_tablename=tmp_ps.alias_tablename;
				if(is_find_head==false){
				     table_rel_node.columnname2=findConnectColumn(tmp_ps.whereString);
				}else {
					 table_rel_node.columnname1=findConnectColumn(tmp_ps.whereString);
				}
		        list_Table_Relationship.add(table_rel_node);
		        is_find_head=true;
			}
		}
		
		if(list_Table_Relationship.size() != 2){
			is_find=false;
			errmsg=" must be 1 head and 1 tail table,which has just one join key.";
			logger.warn(errmsg);
		}
		
		return is_find;
	}
	
	/*
	 * 寻找中间关联的表
	 */
	private int findOtherTable(Table_Relationship last_Relationship,Table_Relationship tail_Relationship,Tree_Node rootnode,Tree_Node top_rootnode)
	{
		//如果是两表join,不需要调用这个函数
		if(list_ParseStruct.size()==2){
			return 0;
		}
		
		//长度检查
		int list_length=list_Table_Relationship.size();
		if(list_length<2){
			logger.warn("less than 2 table.");
			return -1;
		}
		int index=list_Table_Relationship.indexOf(last_Relationship);
		//安全检查
		if(rootnode==null || top_rootnode==null){
			logger.warn("rootNode or top_rootnode is null,please check.");
		    return -1;
		}
		
		if(rootnode.node_type==4)
		{
			findOtherTable(last_Relationship,tail_Relationship,rootnode.left_node,top_rootnode);
			findOtherTable(last_Relationship,tail_Relationship,rootnode.right_node,top_rootnode);
		}else if(rootnode.node_type==2)
		{
			//检查连接键
			if(rootnode.node_content.equals("=") == true
					&& rootnode.right_node.node_content.indexOf(".")>0 
					&& rootnode.right_node.node_content.indexOf("#")<0)
			{
				String cnameString=last_Relationship.alias_tablename+"."+last_Relationship.columnname2;
				//左边精确匹配,则右孩子是另外一个表
				if(rootnode.left_node.node_content.equals(cnameString)==true)
				{
					Table_Relationship tmp_tRelationship=new Table_Relationship();
					String aliastablename=rootnode.right_node.node_content.substring(0, rootnode.right_node.node_content.indexOf("."));
					tmp_tRelationship.tablename=getTablenameByAlias(aliastablename);
					tmp_tRelationship.alias_tablename=aliastablename;
					tmp_tRelationship.columnname1=rootnode.right_node.node_content.substring(rootnode.right_node.node_content.indexOf(".")+1);
					tmp_tRelationship.columnname2=findConnectColumn2(tmp_tRelationship.tablename,tmp_tRelationship.columnname1);
					//作一个检查
					if(tmp_tRelationship.columnname1.equals(tmp_tRelationship.columnname2)){
						errmsg="tablename:"+tmp_tRelationship.tablename+" join key1:"+tmp_tRelationship.columnname1;
						errmsg=errmsg+" join key2:"+tmp_tRelationship.columnname2;
						errmsg=errmsg+" two join key must be different.";
						logger.warn(errmsg);
						return -1;
					}
					if(tmp_tRelationship.tablename.equals(tail_Relationship.tablename)==false){
						list_Table_Relationship.add(index+1,tmp_tRelationship);
						findOtherTable(tmp_tRelationship, tail_Relationship, top_rootnode,top_rootnode);
					}
					else {
						return 0;
					}
				}
				else if(rootnode.right_node.node_content.equals(cnameString)==true)
				{
				    //右边匹配,则左孩子是另外一个表
					Table_Relationship tmp_tRelationship=new Table_Relationship();
					String aliastablename=rootnode.left_node.node_content.substring(0, rootnode.left_node.node_content.indexOf("."));
					tmp_tRelationship.tablename=getTablenameByAlias(aliastablename);
					tmp_tRelationship.alias_tablename=aliastablename;
					tmp_tRelationship.columnname1=rootnode.left_node.node_content.substring(rootnode.left_node.node_content.indexOf(".")+1);
					tmp_tRelationship.columnname2=findConnectColumn2(tmp_tRelationship.tablename,tmp_tRelationship.columnname1);
					//作一个检查
					if(tmp_tRelationship.columnname1.equals(tmp_tRelationship.columnname2)){
						errmsg="tablename:"+tmp_tRelationship.tablename+" join key1:"+tmp_tRelationship.columnname1;
						errmsg=errmsg+" join key2:"+tmp_tRelationship.columnname2;
						errmsg=errmsg+" two join key must be different.";
						logger.warn(errmsg);
						return -1;
					}
					if(tmp_tRelationship.tablename.equals(tail_Relationship.tablename)==false){
						list_Table_Relationship.add(index+1,tmp_tRelationship);
						findOtherTable(tmp_tRelationship, tail_Relationship, top_rootnode,top_rootnode);
					}
					else {
						return 0;
					}
				}
			}
		}
		
		return 0;
	}
	
	//找到第二个连接键columnname2
	private String findConnectColumn2(String tablename, String columnname1) {
		
		String whereString="";
		String columnname2="";
		String groupString=columnname1+":=:1";
		for(int i=0;i<list_ParseStruct.size();i++)
		{
			if(list_ParseStruct.get(i).tablename.equals(tablename)==true)
			{
				whereString=list_ParseStruct.get(i).whereString;
				break;
			}
		}
		//这里处理一种特殊情况
		if(whereString.indexOf(";")<0){
			logger.debug("findConnectColumn2 function: columnname1:"+columnname1+" columnname2:"+columnname2);
			return columnname2;
		}
		
		logger.debug("findConnectColumn2 function:whereString:"+whereString);
		
		//在whereString中删除columnname1
		whereString=deleteColumn1InWhereStr(whereString,groupString);
		
		columnname2=findConnectColumn(whereString);
		logger.debug("findConnectColumn2 function:whereString:"+whereString);
		logger.debug("findConnectColumn2 function: columnname1:"+columnname1+" columnname2:"+columnname2);
		return columnname2;
	}

	//在whereString中删除groupString
	private String deleteColumn1InWhereStr(String whereString,
			String groupString) {
		//中间匹配
		if(whereString.indexOf(";"+groupString+";") > 0){
			whereString=whereString.replace(";"+groupString+";", ";");
		}else if(whereString.indexOf(groupString+";") >= 0){
			//头匹配
			whereString=whereString.replace(groupString+";", "");
		}else if(whereString.indexOf(";"+groupString) > 0){
			//尾匹配
			whereString=whereString.replace(";"+groupString, "");
		}
		return whereString;
	}

	private String findConnectColumn(String whereString) {
		String columnname="";
		String [] columnStrings=whereString.split(";");
		for(int i=0;i<columnStrings.length;i++)
		{
			String[] sub_columnStrings=columnStrings[i].split(":");
			if(sub_columnStrings[1].equals("=") && sub_columnStrings[2].equals("1"))
			{
				columnname=sub_columnStrings[0];
				break;
			}
		}
		return columnname;
	}

	/*
	 * 根据表的别名,找表的真名
	 */
	private String getTablenameByAlias(String alias_tablename)
	{
		String tablename="";
		for(Iterator<ParseStruct> r=list_ParseStruct.iterator();r.hasNext();)
		{
			ParseStruct tmp_ps=r.next();
			if(tmp_ps.alias_tablename.equals(alias_tablename)){
				tablename=tmp_ps.tablename;
				break;
			}
		}
		return tablename;
	}
	//将树中的条件找出来,拼接到whereString里面
	//拼接格式column_name:operator:is_join_key;column_name:operator:is_join_key
	private void fillwhereString(ParseStruct ps, Tree_Node rootnode) {
	    if(ps.whereString==null) {
	    	ps.whereString="";
	    }
		if(rootnode==null)
		{
			logger.warn("fillwhereString function: rootnode is null.ps.tablename="+ps.tablename+" ps.whereString="+ps.whereString);
			return;
		}
		if(rootnode.node_type==4){
			fillwhereString(ps,rootnode.left_node);
			fillwhereString(ps,rootnode.right_node);
		}else if(rootnode.node_type==2) {
			//看看right child value中是否含有.,并且不含有#,则认为是连接键
			if(rootnode.node_content.equals("=") == true
					&& rootnode.right_node.node_content.indexOf(".")>0 
					&& rootnode.right_node.node_content.indexOf("#")<0){
				//这个结点存在连接键,看看左右孩子,哪个匹配别名
				if(rootnode.left_node.node_content.indexOf(ps.alias_tablename+".") >= 0){
			           ps.whereString=ps.whereString+rootnode.left_node.node_content+":"+rootnode.node_content+":1;";
				}else if(rootnode.right_node.node_content.indexOf(ps.alias_tablename+".") >=0){
					   ps.whereString=ps.whereString+rootnode.right_node.node_content+":"+rootnode.node_content+":1;";
				}
			}else {
				//只需检查一下左孩子,并且这个列是非连接键
				if(rootnode.left_node.node_content.indexOf(ps.alias_tablename+".") >= 0){
				       ps.whereString=ps.whereString+rootnode.left_node.node_content+":"+rootnode.node_content+":0;";
				}
			}
		}
		
	}

	//寻找whereString的结束位置
	public int FindWhereEndPostion(String sqlString)
	{
		if(sqlString==null || sqlString.length()==0)
			return -1;
		int min=sqlString.length();
		int addr_order_by=sqlString.indexOf("order by");
		int addr_group_by=sqlString.indexOf("group by");
		int addr_limit=sqlString.indexOf(" limit ");
		if(addr_order_by>0)
		{
			if(min > addr_order_by){
				min = addr_order_by;
			}
		}
		if(addr_group_by>0)
		{
			if(min > addr_group_by){
				min = addr_group_by;
			}
		}
		if(addr_limit>0)
		{
			if(min > addr_limit){
				min = addr_limit;
			}
		}
		
		return min;
	}
	
	//把group by columns找出来
	public int AnalyzeGroupByStr()
	{
		logger.debug("enter function AnalyzeGroupByStr");
		String groupbyString="";
		int addr_group_by=sql.indexOf("group by");
		if(addr_group_by<0){
			return 0;
		}
	
		if(sql.indexOf("having", addr_group_by+8) > 0)
		{
			groupbyString=sql.substring(addr_group_by+8, sql.indexOf("having", addr_group_by+8)).trim();
		}
		else if(sql.indexOf("order by", addr_group_by+8)>0)
		{
			groupbyString=sql.substring(addr_group_by+8, sql.indexOf("order by", addr_group_by+8)).trim();
			
		}
		else if(sql.indexOf("limit",addr_group_by+8)>0){
			groupbyString=sql.substring(addr_group_by+8, sql.indexOf("limit", addr_group_by+8)).trim();
		}
		
		if(groupbyString.length()==0) {
			errmsg="group by has syntax error.";
			logger.warn(errmsg);
			return -1;
		}
		
		logger.debug("groupbycolumns:"+groupbyString);
		String [] group_by_columns = groupbyString.split(",");
		for(Iterator<ParseStruct> r=list_ParseStruct.iterator();r.hasNext();)
		{
			ParseStruct tmp_ps=r.next();
			tmp_ps.groupbycolumn="";
			for(int i=0;i<group_by_columns.length;i++)
			{
				if(group_by_columns[i].trim().indexOf(tmp_ps.alias_tablename+".")>=0){
					//找到
					if(tmp_ps.groupbycolumn.length()==0){
					    tmp_ps.groupbycolumn=group_by_columns[i].trim();
					}
					else{
						tmp_ps.groupbycolumn=tmp_ps.groupbycolumn+","+group_by_columns[i].trim();
					}
				}
			}
			
			tmp_ps.groupbycolumn=tmp_ps.groupbycolumn.replace(tmp_ps.alias_tablename+".", "");
			logger.debug("table name:"+tmp_ps.tablename);
			logger.debug("alias table name:"+tmp_ps.alias_tablename);
			logger.debug("group by columns:"+tmp_ps.groupbycolumn);
		}
		
		return 0;
	}
	
	//把order by columns字段找出来
	public int AnalyzeOrderByStr()
	{
		logger.debug("enter function AnalyzeOrderByStr");
		String orderbycolumns="";
		int addr_order_by=sql.indexOf("order by");
		if(addr_order_by<0) return 0;
		
		if(sql.indexOf(" limit ",addr_order_by) > addr_order_by)
		{
			orderbycolumns=sql.substring(addr_order_by+8, sql.indexOf(" limit ",addr_order_by));
		}
		else {
			//no limit key word
			orderbycolumns=sql.substring(addr_order_by+8).trim();
		}
		
		orderbycolumns=orderbycolumns.replace(" asc", " ");
		orderbycolumns=orderbycolumns.replace(" desc", " ");
		
		logger.debug("orderbycolumns:"+orderbycolumns);
		
		String [] order_by_columns = orderbycolumns.split(",");
		for(Iterator<ParseStruct> r=list_ParseStruct.iterator();r.hasNext();)
		{
			ParseStruct tmp_ps=r.next();
			tmp_ps.orderbycolumn="";
			for(int i=0;i<order_by_columns.length;i++)
			{
				if(order_by_columns[i].trim().indexOf(tmp_ps.alias_tablename+".")>=0){
					//找到
					if(tmp_ps.orderbycolumn.length()==0){
					    tmp_ps.orderbycolumn=order_by_columns[i].trim();
					}
					else{
						tmp_ps.orderbycolumn=tmp_ps.orderbycolumn+","+order_by_columns[i].trim();
					}
				}
			}
			
			tmp_ps.orderbycolumn=tmp_ps.orderbycolumn.replace(tmp_ps.alias_tablename+".", "");
			logger.debug("table name:"+tmp_ps.tablename);
			logger.debug("alias table name:"+tmp_ps.alias_tablename);
			logger.debug("order by columns:"+tmp_ps.orderbycolumn);
		}
		
		return 0;
	}
	
	//外部只需要调这个函数,就可以初始化list_ParseStruct这个结构体
	//以及list_Table_Relationship这个结构体
	public void ParseComplexSQL()
	{
		//计算三个关键位置
		int addr_from=sql.indexOf(" from ");
		int addr_where=sql.indexOf(" where ");
		int where_end=FindWhereEndPostion(sql);
		logger.info("alanyze mutiple table join begin.");
		//获得表,以及表的别名
		if(AnalyzeMutipleTable(sql.substring(addr_from+6, addr_where))<0){
			return;
		}
		//获得每个表的where条件
		if(AnalyzeWhereStr(sql.substring(addr_where+7, where_end).trim())<0){
			return;
		}
		//获得每个表的group by columns
		if(AnalyzeGroupByStr()<0){
			return;
		}
		//获得每个表的order by columns
		if(AnalyzeOrderByStr()<0){
			return;
		}
		logger.info("alanyze mutiple table join success.");
	}

}
