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


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;


/*
 * function: create index for these sqls
 *          
 */

public class CreateIndex {
	//log4j日志
	private static Logger logger = Logger.getLogger(CreateIndex.class);
	//SQL REVIEW DATABASE操作对象
	IHandleDB wtb;
	//所有的SQL审核语句
	List<SQL_Node> list_sql;
	//元数据相关对象
	IMetaData md;
	//默认元数据构造方式为0,这个不用构造全库的元数据
	int metaDataBuildtype=0;
	//创建索引过程中的错误和建议
	String auto_review_error;
	String auto_review_tip;
	
	//每个条件列的势从大到小排序
	class Column_Card
	{
		String column_name;
		int Cardinality;
		AnalyzeColumnStructure acs;
	}
	
    //分析的基本结构
    class AnalyzeColumnStructure
    {
    	String column_name; //列名
    	String column_type; //列的类型
    	int column_type_score; //类型得分
    	String is_null_able;
    	String symbol;   //运算符
    	int symbol_score; //运算符分数
    	int Cardinality; //不同值的个数,这是一个抽样值
    	int Cardinality_score; //集的势的得分
    	int total_score; //总分,这个与最后列在索引中的排序直接相关
        int type; //select 字段 0,where字段1,group by字段2,order by字段3
        boolean exist_index; //是否已在索引列里
        List<Index_Node> list_index;
        boolean is_order_column; //是否是排序字段
        int is_join_key; //是否是连接键
        
        
        public AnalyzeColumnStructure()
        {
        	is_order_column=false;
        	column_type_score=0;
        	symbol_score=0;
        	Cardinality_score=0;
        	total_score=0;
        	is_join_key=0;
        }
    }
    
    //分析的单表结构
    class AnalyzeTableStructure
    {
    	String tablename;
    	//分库分表
    	String real_tablename;
    	List<AnalyzeColumnStructure> list;
    	
    	public  AnalyzeTableStructure(String tablename) {
    		this.tablename=tablename;
    		this.real_tablename=tablename;
			list = new LinkedList<AnalyzeColumnStructure>();
		}
    }
    
    //分析多表的结构
    class AnalyzeMTableStructure
    {
    	List<AnalyzeTableStructure> list;
    	public AnalyzeMTableStructure()
    	{
    		list = new LinkedList<AnalyzeTableStructure>();
    	}
    }
    
    //构造函数
	public CreateIndex()
	{
		this.wtb=new HandleSQLReviewDB();
		this.md = new MySQLMetaData();
	}
	
	//前端调用的构造函数
	public CreateIndex(String IP,int port,String dbname,String user,String password,IHandleDB ihandleSQLReviewDB){
		this.md = new MySQLMetaData(IP,port,dbname,user,password);
		this.wtb=ihandleSQLReviewDB;
	}
	
	//获得所有的SQL
	public void getAllSQL() throws SQLException
	{
		list_sql=wtb.getAllSQL();
	}
	//获得所有的SQL
	public void getAllSQL(int sqlmapFileID) throws SQLException
	{
		list_sql=wtb.getAllSQL(sqlmapFileID);
	}
	
	//获得这个库的元数据
	public void getMetaData()
	{
	    md.buildDBMetaData();
	}
	
	//分析每一个SQL
	public void reviewSQL()
	{
		String sql;
		int sql_id;
		String tablenames="";
		if(metaDataBuildtype != 0)
		{
			//一次性全库构造元数据
			getMetaData();
		}
		
		for(Iterator<SQL_Node> r=list_sql.iterator();r.hasNext();)
    	{
			auto_review_error="";
			auto_review_tip="";
			tablenames="";
			SQL_Node sql_node= r.next();
    		sql = sql_node.sqlString;
    		sql_id =  sql_node.id;
    		logger.info("待审核的SQL_ID="+sql_id+",原始SQL TEXT为:"+sql);
    		ParseSQL ps = new ParseSQL(sql);
    		ps.sql_dispatch();
    		//检查sql parse的结果
    		auto_review_tip=ps.tip;
    		if(checkParseResult(ps.errmsg,ps.sql_type,sql_id)==false){
    			continue;
    		}
    		
    		if(ps.tag==0)
    		{
    			    tablenames=ps.tablename;
    			    //单表填冲元数据
    			    if(fillTableMetaData(ps.tablename,sql_id)==false){
    			    	continue;
    			    }
		    		AnalyzeTableStructure ats = new AnalyzeTableStructure(ps.tablename);
		    		modifyAtsRealTablename(ats,ps.tablename);
		    		try {
		    			//将数据从where树中填入到ats中
		        		LoadWhereDataToACS(ps.whereNode,ats);
		        		if(auto_review_error.length()>0){
		        			wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip,tablenames);
		        			continue;
		        		}
		        		//当前是把group by与order by column一起当做排字段来处理
		        		String orderString=contactGroupbyOrderby(ps.groupbycolumn,ps.orderbycolumn);
		        		//将排序字段元数据装载ats
		        		LoadOrderDataToACS(orderString,ats,ps.map_columns);
		        		if(auto_review_error.length()>0){
		        			wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip,tablenames);
		        			continue;
		        		}
		        		//检查排序字段的长度
		        		auto_review_tip=checkOrderByColumnsLength(orderString,ats);
		        		//计算最优索引
		        		String createindexString=ComputeBestIndex(ats);
		        		//将这条SQL的审核结果保存到数据库中
		        		wtb.updateSQLStatus(1, createindexString, sql_id,auto_review_error,auto_review_tip,tablenames);
					} catch (Exception e) {
						logger.error("the process of creating index has some errors:", e);
					}
    		}else if(ps.tag==1 && ps.sql_type==3){
    			//多表select语句的parse
    			boolean find_all_table_metadata=true;
    			ParseMutiTableSQL pmts = new ParseMutiTableSQL(sql);
    			pmts.ParseComplexSQL();
    			//错误检查
    			auto_review_error=pmts.errmsg;
    			if(auto_review_error.length()>0){
    				wtb.updateSQLStatus(2, "", sql_id,auto_review_error,auto_review_tip);
    				logger.warn("current sql:"+sql+" parse has some errors:"+auto_review_error);
    				continue;
    			}
    			
    			//获得所有表名
    			for(Iterator<ParseStruct> iter=pmts.list_ParseStruct.iterator();iter.hasNext();)
    			{
    				if(tablenames.equals("")==true){
    					tablenames=iter.next().tablename;
    				}else {
						tablenames=tablenames+","+iter.next().tablename;
					}
    			}
    			
    			//获得这条SQL所涉及的表的所有元数据
    			for(Iterator<ParseStruct> iter=pmts.list_ParseStruct.iterator();iter.hasNext();)
    			{
    				ParseStruct tmp_ps=iter.next();
    			    if(fillTableMetaData(tmp_ps.tablename,sql_id)==false){
    			    	find_all_table_metadata=false;
    			    	break;
    			    }
    				
    			}
    			if(find_all_table_metadata==false){
    				continue;
    			}
    			
    			//声明单条SQL多表相关元数据保存对象
    			AnalyzeMTableStructure amts=new AnalyzeMTableStructure();
    			String createindexString2="";
    			for(Iterator<ParseStruct> iter2=pmts.list_ParseStruct.iterator();iter2.hasNext();)
    			{
    				ParseStruct tmp_ps2=iter2.next();
    				AnalyzeTableStructure tmp_ats=new AnalyzeTableStructure(tmp_ps2.tablename);
    				modifyAtsRealTablename(tmp_ats,tmp_ps2.tablename);
    				LoadWhereDataToACS(tmp_ps2.whereString,tmp_ats);
    				if(auto_review_error.length()>0){
	        			wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip,tablenames);
	        			continue;
	        		}
    				//当前是把group by与order by column一起当做排字段来处理
	        		String orderString2=contactGroupbyOrderby(tmp_ps2.groupbycolumn,tmp_ps2.orderbycolumn);
	        		//将排序字段元数据装载tmp_ats
	        		LoadOrderDataToACS(orderString2,tmp_ats,null);
	        		if(auto_review_error.length()>0){
	        			wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip,tablenames);
	        			continue;
	        		}
	        		//检查排序字段的长度
	        		String checkorderby=checkOrderByColumnsLength(orderString2,tmp_ats);
	        		if(checkorderby.length()>0){
	        			auto_review_tip=checkorderby;
	        		}
	        		//添加进链表
	        		amts.list.add(tmp_ats);
    			}
    			
    			//计算驱动表
    			ComputeDriverTable(amts, pmts.list_Table_Relationship);
    			//计算每个表的最优索引
    			for(int i=0;i<pmts.list_Table_Relationship.size();i++)
    			{
    				AnalyzeTableStructure ats3=getATSByTablename(amts, pmts.list_Table_Relationship.get(i).tablename);
    				String tmp_best_index=ComputeBestIndex(ats3,pmts.list_Table_Relationship.get(i));
    				//拼接createindexString2
	        		if(createindexString2.length()==0){
	        			createindexString2=tmp_best_index;
	        		}else {
	        			createindexString2=createindexString2+tmp_best_index;
	        		}
    			}
    			
    			logger.info("muti-table sql all index:"+createindexString2);
    			//将这条SQL的审核结果保存到数据库中
        		wtb.updateSQLStatus(1, createindexString2, sql_id,auto_review_error,auto_review_tip,tablenames);
    			
    		}
    	}//end for
	}
	
    /*
     * 分库分表的情况下,需要修改ats中的real_tablename变量的值
     */
	private void modifyAtsRealTablename(AnalyzeTableStructure ats,
			String tablename) 
	{
		String real_tablename=md.findMatchTable(tablename);
		if(real_tablename != null && real_tablename.equals(tablename)==false){
			ats.real_tablename=real_tablename;
		}
	}

	/*
	 * 计算驱动表,支持两表及两表以上
	 * 
	 */
	private int ComputeDriverTable(AnalyzeMTableStructure amts,List<Table_Relationship> list_table_relationship) 
	{
		//长度一致性检查
		int amts_size=amts.list.size();
		int list_table_relationship_size=list_table_relationship.size();
		if(amts_size != list_table_relationship_size){
			logger.warn("AnalyzeMTableStructure ,List<Table_Relationship> list size() has some difference.");
			return -1;
		}
		if(list_table_relationship_size<2){
			logger.warn("list_table_relationship list size less than 2.");
			return -1;
		}
		
		//计算list_table_relationship list头和尾的Table的card,中间的表不需要计算Card
		AnalyzeTableStructure ats_head=getATSByTablename(amts,list_table_relationship.get(0).tablename);
		AnalyzeTableStructure ats_tail=getATSByTablename(amts,list_table_relationship.get(list_table_relationship_size-1).tablename);
		ComputeTableCard(ats_head,list_table_relationship.get(0));
		ComputeTableCard(ats_tail,list_table_relationship.get(list_table_relationship_size-1));
		logger.debug("list_table_relationship first tablename "+list_table_relationship.get(0).tablename+" table Card:"+list_table_relationship.get(0).Cardinality);
		logger.debug("list_table_relationship last tablename "+list_table_relationship.get(list_table_relationship_size-1).tablename+" table Card:"+list_table_relationship.get(list_table_relationship_size-1).Cardinality);
		if(list_table_relationship.get(0).Cardinality < list_table_relationship.get(list_table_relationship_size-1).Cardinality)
		{
			//将list_table_relationship中的顺序反一下,排在第一个位置的我们认为是整个SQL的驱动表
	        reverseListTableRelationship(list_table_relationship);
	        logger.debug("reverse list_table_relationship list.Result is as follows:");
	        logger.debug("list_table_relationship first tablename "+list_table_relationship.get(0).tablename+" table Card:"+list_table_relationship.get(0).Cardinality);
			logger.debug("list_table_relationship last tablename "+list_table_relationship.get(list_table_relationship_size-1).tablename+" table Card:"+list_table_relationship.get(list_table_relationship_size-1).Cardinality);
	        
		}else{
			logger.debug("no need to reverse list_table_relationship list.");
		}
		return 0;
	}
	
	/*
	 * 将链表反转
	 */
	private void reverseListTableRelationship(List<Table_Relationship> list_table_relationship) 
	{	
		//声明一个临时链表
		List<Table_Relationship> tmpList=new LinkedList<Table_Relationship>();
		//将原链表中的所有数据,先保存到另外一个链表中
		for(int i=list_table_relationship.size()-1;i>=0;i--)
		{
			tmpList.add(list_table_relationship.get(i));
			list_table_relationship.remove(i);
		}
		
		//反转连接键,并把element重新添加进链表里面
		for(int j=0;j<tmpList.size();j++)
		{
			String tmp_col=tmpList.get(j).columnname1;
			tmpList.get(j).columnname1=tmpList.get(j).columnname2;
			tmpList.get(j).columnname2=tmp_col;
			list_table_relationship.add(tmpList.get(j));
		}
	}

	/*
	 * 通过表名,来找对应的AnalyzeTableStructure
	 */
	private AnalyzeTableStructure getATSByTablename(AnalyzeMTableStructure amts,String tablename) 
	{
		boolean is_find=false;
		AnalyzeTableStructure ats = null;
		for(Iterator<AnalyzeTableStructure> iterator=amts.list.iterator();iterator.hasNext();)
		{
			ats=iterator.next();
			if(ats.tablename.equals(tablename)==true)
			{
				is_find=true;
				break;
			}
		}
		
		if(is_find){
			return ats;
		}else {
			logger.warn("can't find AnalyzeTableStructure by tablename:"+tablename);
			return null;
		}
	}

	/*
	 * 计算一个表的Cardinality
	 */
	private int ComputeTableCard(AnalyzeTableStructure ats,Table_Relationship tRelationship) 
	{
		int card=1;
		boolean zero=true;
		String join_columnString="";
		int list_size=ats.list.size();
		
		if(ats.tablename.equals(tRelationship.tablename)==false){
			logger.error("ComputeTableCard中ats与tRelationship表名不一致.");
			return -1;
		}
		
		for(int i=0;i<list_size;i++)
		{
			AnalyzeColumnStructure acs=ats.list.get(i);
			if(acs.is_join_key==0)
			{
				//不计算排序字段的Cardinality
				if(acs.is_order_column==false)
				{
					if(acs.symbol.equals("=")==true){
						zero=false;
						card=card*acs.Cardinality;
					}else if(acs.symbol.equals(">")==true){
						zero=false;
						card=card*acs.Cardinality;
					}else if(acs.symbol.equals(">=")==true){
						zero=false;
						card=card*acs.Cardinality;
					}else if(acs.symbol.equals("<")==true){
						zero=false;
						card=card*acs.Cardinality;
					}else if(acs.symbol.equals("<=")==true){
						zero=false;
						card=card*acs.Cardinality;
					}else if(acs.symbol.equals("in")==true){
						zero=false;
						card=card*acs.Cardinality;
					}
				}
			}
			else {
				if(join_columnString.length()>0){
					logger.error("table "+ats.tablename+" has two or more join columns.");
					return -1;
				}else{
				     join_columnString=acs.column_name;
				}
			}
		}
		//对于新项目的表,很容易出现此情况,这种会导致计算驱动表不准确
		if(card==0){
			logger.warn("table "+ats.tablename+"has no data. cardinality is 0.");
		}
		
		//除连接键外,没有任何条件的Cardinality
		if(zero){
			card=0;
		}
		
		tRelationship.Cardinality=card;
		
		//正确返回
		return 0;
	}

	//装载多表where string的元数据
	//whereString数据格式column_name:operator:is_join_key;column_name:operator:is_join_key
	private void LoadWhereDataToACS(String whereString,AnalyzeTableStructure ats) 
	{
		String [] Columns=whereString.split(";");
		for(int i=0;i<Columns.length;i++)
		{
			    String column_name=Columns[i].substring(0, Columns[i].indexOf(":"));
			    String operator=Columns[i].substring(Columns[i].indexOf(":")+1,Columns[i].lastIndexOf(":"));
			    int is_join_key=Integer.valueOf(Columns[i].substring(Columns[i].lastIndexOf(":")+1));
				AnalyzeColumnStructure acs = new AnalyzeColumnStructure();
				acs.symbol=operator;
				//计算操作符分值,等号分值最高,这个分值暂时没有用
				if(acs.symbol.equals("="))
					acs.symbol_score = 10;
				else {
					acs.symbol_score = 5;
				}
				acs.column_name=column_name;
				//查找这一列的元数据
				Column_Node cn=md.searchColumnMetaData(ats.tablename, acs.column_name);
				if(cn==null){
					auto_review_error="Table:"+ats.tablename+" Column:"+acs.column_name+" does not exist.";
					logger.warn(auto_review_error);
					return;
				}
				//查找这个列所在的索引元数据
				acs.list_index = md.searchIndexMetaData(ats.tablename, acs.column_name);
				acs.column_type=cn.column_type;
				//分值暂时没有用
				acs.column_type_score=100;
				acs.is_null_able=cn.is_nullable;
				acs.Cardinality=cn.sample_card;
				//分值暂时没有用
				acs.Cardinality_score=100;
				if(acs.list_index.isEmpty()==true)
					acs.exist_index=false;
				else {
					acs.exist_index=true;
				}
				acs.type=1;
				//标示连接键
				if(is_join_key==1){
					acs.is_join_key=1;
				}
				
				//将列的所有信息添加到table中,准备后续的计算使用
				if(!checkExistAcsInAts(acs,ats)){
				    ats.list.add(acs);
				}
		}
		
	}

	/*
	 * 检查是否已存在的acs,只需要看看列名是否存在
	 */
	private boolean checkExistAcsInAts(AnalyzeColumnStructure acs,
			AnalyzeTableStructure ats) {
		if(acs==null || ats==null){
			logger.warn("checkExistAcsInAts:acs null or ats null.");
			return false;
		}
		for(Iterator<AnalyzeColumnStructure> iterator=ats.list.iterator();iterator.hasNext();)
		{
			if(acs.column_name.equals(iterator.next().column_name)){
				return true;
			}
		}
		return false;
	}

	//检查parse中的错误,以及一些sql_type信息
	private boolean checkParseResult(String errmsg, int sql_type,int sql_id) 
	{
		if(errmsg.length()>0)
		{
			auto_review_error="此SQL在语法解析时出现如下的错误:"+errmsg;
			wtb.updateSQLStatus(2, "", sql_id,auto_review_error,auto_review_tip);
			logger.error("at the process of SQL parse,some errors happens:"+errmsg);
			return false;
		}
		
		if(sql_type==0)
		{
			auto_review_tip="insert语句,不需要进行审核";
			wtb.updateSQLStatus(1, "", sql_id,auto_review_error,auto_review_tip);
			logger.info("insert SQL doesn't need to sql review.");
			return false;
		}
		
		if(sql_type==-1)
		{
			auto_review_error="无法识别的SQL语句类型,当前支持select,insert,update,delete语句";
			wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip);
			logger.error("can't recongnize the sql type. now ,support select,insert,update,delete SQL statement");
			return false;
		}
		
		return true;
	}

	/*
	 * 填冲单表的元数据
	 */
	private boolean fillTableMetaData(String tablename,int sql_id) 
	{
		//获得最相近的表名,支持分库分表
		String real_tablename=md.findMatchTable(tablename);
		if(real_tablename==null)
		{
			 //检查在目标数据库中是否真的存在
			 logger.warn(tablename+" 元数据无法构造. because the table doesn't exist.");
		     auto_review_error= "Error:table "+tablename+" does not exist.";
		     wtb.updateSQLStatus(2,"", sql_id,auto_review_error,auto_review_tip);
			 return false;
		}else if(md.checkTableExist(tablename)==false)
		{
			//检查在Cache中是否存在
			if(metaDataBuildtype==0)
    		{
    		   md.buildTableMetaData(tablename);
    		}
		}
		
		if(real_tablename.equals(tablename)==false)
		{
		   //分库分表,表名替换tip
			auto_review_tip="Table:"+tablename+"分库分表,元数据加载表名替换为:"+real_tablename;
			logger.info(auto_review_tip);
			wtb.updateSQLStatus(0,"", sql_id,auto_review_error,auto_review_tip);
		}
		
		return true;
	}
	/*
	 * 把group by columns与order by columns字段拼起来
	 */
	private String contactGroupbyOrderby(String groupbycolumn,String orderbycolumn) {
		String orderString="";
		if(groupbycolumn.length()>0)
		{
			orderString=groupbycolumn;
		}
		if(orderbycolumn.length()>0)
		{
			if(orderString.length()>0)
			{
				orderString=orderString+","+orderbycolumn;
			}
			else {
				orderString=orderbycolumn;
			}
		}
		
		return orderString;
	}

	/*
	 * 检查排序字段的长度,比如待排序字段为order by a,b
	 * 在数据字典中a的类型定义为varchar(1000),b的定义为varchar(1000)
	 * 如果这个查询语句查出来的记录数又很多,那么对sort_buffer_size需要调大,否则会报错
	 */
	private String checkOrderByColumnsLength(String orderbycolumn,AnalyzeTableStructure ats) 
	{
		String column_name;
		String column_type;
		int varchar_length;
		AnalyzeColumnStructure acs;
		String checkError="";
		boolean is_find=false;
		if(orderbycolumn.length()==0) return checkError;
		String[] tmp = orderbycolumn.split(",");
		for(int i=0;i<tmp.length;i++)
		{
			column_name=tmp[i];
			for(Iterator<AnalyzeColumnStructure> r=ats.list.iterator();r.hasNext();)
			{
				acs=r.next();
				if(acs.column_name.equals(column_name))
				{
					column_type=acs.column_type;
					if(column_type.length()>7 && column_type.substring(0, 7).equals("varchar")==true)
					{
						int start=column_type.indexOf("(");
						int end=column_type.indexOf(")");
						varchar_length = Integer.valueOf(column_type.substring(start+1,end));
						if(varchar_length > 200)
						{
							is_find=true;
							logger.warn("order by column:"+column_name+"  column type:"+column_type+" is bigger than varchar(200). there has some danger in sort buffer size.");
						}
					}
					break;
				}
			}
		}
		
		if(is_find==true)
		{
			checkError="order by column type is so big.";
		}
		return checkError;
	}
	
	/*
	 * 构造建索引的脚本
	 */
	private String BuildCreateIndexScript(String index_columns,AnalyzeTableStructure ats)
	{
		String index_name;
		String createIndexScript="";
		if(index_columns.indexOf(",")>0)
		{
		    index_name="idx_"+ats.tablename+"_"+index_columns.substring(0, index_columns.indexOf(","));
		}
		else {
			index_name="idx_"+ats.tablename+"_"+index_columns;
		}
		
		if(ats.tablename.equals(ats.real_tablename)){
			createIndexScript="create index "+index_name+" on "+ats.tablename+"("+index_columns+");";
		}else{
			//分库分表
			createIndexScript=ats.tablename+"分库分表   create index "+index_name+" on "+ats.real_tablename+"("+index_columns+");";
		}
		
		if(index_columns.length()==0)
		{
			        logger.warn("没有任何where条件字段,不创建索引");
		}
		else {
					logger.info("create index script: "+createIndexScript);
		}	
		
		return createIndexScript;
	}
	
	/*
	 * 计算单表最优索引
	 */
	private String ComputeBestIndex(AnalyzeTableStructure ats) {
		//第一种规则
		//看看where条件字段中是否有主键字段,如果有则直接用主键索引
		AnalyzeColumnStructure acs;
		boolean is_find_best_index=false;
		
		for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
		{
			acs=r.next();
			//非排序字段
			if(acs.is_order_column==true) continue;
			//检查索引
			if(acs.list_index != null && acs.symbol.equals("!=")==false)
			{
				for(Iterator<Index_Node> in=acs.list_index.iterator();in.hasNext();)
				{
					Index_Node tmp_iNode=in.next();
					if(tmp_iNode.index_name.equals("PRIMARY") && tmp_iNode.seq_in_index==1)
					{
						logger.info(acs.column_name+"有PRIMARY KEY索引,直接使用");
						is_find_best_index=true;
						break;
					}
				}
			}
			
			if(is_find_best_index==true) break;
		}
		//含有主键索引字段,则直接退出
		if(is_find_best_index==true) return "PRIMARY";
		
		
		//第二种规则
		//看看条件字段中是否有唯一键字段,如果有则直接用唯一键索引
		for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
		{
			acs=r.next();
			//非排序字段
			if(acs.is_order_column==true) continue;
			//检查索引
			if(acs.list_index != null && acs.symbol.equals("!=")==false)
			{
				for(Iterator<Index_Node> in=acs.list_index.iterator();in.hasNext();)
				{
					Index_Node tmp_iNode=in.next();
					if(tmp_iNode.non_unique==0 && tmp_iNode.seq_in_index==1)
					{
						logger.info(acs.column_name+"有UNIQUE KEY索引,直接使用");
						is_find_best_index=true;
						break;
					}
				}
			}
			
			if(is_find_best_index==true) break;
		}
		//含有唯一键索引前导列字段,则直接退出
		if(is_find_best_index==true) return "UNIQUE KEY";
		
		
		//第三种规则,此表除了主键,唯一键以外,计算最优索引
		//计算针对本SQL的最优索引
		List<Column_Card> list_card_denghao = new ArrayList<Column_Card>();
		List<Column_Card> list_card_no_denghao = new ArrayList<Column_Card>();
		List<Column_Card> list_card_order = new ArrayList<Column_Card>();
		int type; //取值0,1,2    0代表等号操作的column,这个具有最高的优先级,1为非等号,2为排序字段
		type=0;
		SortColumnCard(list_card_denghao,ats,type);
		type=1;
		SortColumnCard(list_card_no_denghao,ats,type);
		type=2;
		SortColumnCard(list_card_order,ats,type);
		
		String index_columns=BuildBTreeIndex(list_card_denghao,list_card_no_denghao,list_card_order);
		String createIndexScript=BuildCreateIndexScript(index_columns,ats);	
		//构建创建索引的脚本
		return createIndexScript;
	}

	/*
	 * 
	 * 多表joinSQL计算每个单表的最优索引
	 */
	private String ComputeBestIndex(AnalyzeTableStructure ats,
			                        Table_Relationship table_Relationship) {
		//第一种规则
		//看看where条件字段中是否有主键字段,如果有则直接用主键索引
		AnalyzeColumnStructure acs;
		boolean is_find_best_index=false;
		
		for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
		{
			acs=r.next();
			//非排序字段
			if(acs.is_order_column==true) continue;
			//连接字段columnname2不能作为条件
			if(acs.is_join_key==1 && acs.column_name.equals(table_Relationship.columnname2)==true){
				continue;
			}
			//检查索引
			if(acs.list_index != null 
					&& acs.symbol.equals("!=")==false)
			{
				for(Iterator<Index_Node> in=acs.list_index.iterator();in.hasNext();)
				{
					Index_Node tmp_iNode=in.next();
					if(tmp_iNode.index_name.equals("PRIMARY") && tmp_iNode.seq_in_index==1)
					{
						logger.info(acs.column_name+"有PRIMARY KEY索引,直接使用");
						is_find_best_index=true;
						break;
					}
				}
			}
			
			if(is_find_best_index==true) break;
		}//end for
		
		//含有主键索引字段,则直接退出
		if(is_find_best_index==true) return ats.tablename+" has PRIMARY index;";
		
		
		//第二种规则
		//看看条件字段中是否有唯一键字段,如果有则直接用唯一键索引
		for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
		{
			acs=r.next();
			//非排序字段
			if(acs.is_order_column==true) continue;
			//连接字段columnname2不能作为条件
			if(acs.is_join_key==1 && acs.column_name.equals(table_Relationship.columnname2)==true){
				continue;
			}
			//检查索引
			if(acs.list_index != null && acs.symbol.equals("!=")==false)
			{
				for(Iterator<Index_Node> in=acs.list_index.iterator();in.hasNext();)
				{
					Index_Node tmp_iNode=in.next();
					if(tmp_iNode.non_unique==0 && tmp_iNode.seq_in_index==1)
					{
						logger.info(acs.column_name+"有UNIQUE KEY索引,直接使用");
						is_find_best_index=true;
						break;
					}
				}
			}
			
			if(is_find_best_index==true) break;
		}
		//含有唯一键索引前导列字段,则直接退出
		if(is_find_best_index==true) return ats.tablename+" has UNIQUE KEY index;";
		
		
		//第三种规则,此表除了主键,唯一键以外,计算最优索引
		//计算针对本SQL的最优索引
		List<Column_Card> list_card_denghao = new ArrayList<Column_Card>();
		List<Column_Card> list_card_no_denghao = new ArrayList<Column_Card>();
		List<Column_Card> list_card_order = new ArrayList<Column_Card>();
		int type; //取值0,1,2    0代表等号操作的column,这个具有最高的优先级,1为非等号,2为排序字段
		type=0;
		SortColumnCard(list_card_denghao,ats,type,table_Relationship);
		type=1;
		SortColumnCard(list_card_no_denghao,ats,type);
		type=2;
		SortColumnCard(list_card_order,ats,type);
		
		String index_columns=BuildBTreeIndex(list_card_denghao,list_card_no_denghao,list_card_order);
		String createIndexScript=BuildCreateIndexScript(index_columns,ats);	
		//构建创建索引的脚本
		return createIndexScript;
	}
	/*
	 * 返回最佳索引组合
	 */
	private String BuildBTreeIndex(List<Column_Card> list_card_denghao,
			                      List<Column_Card> list_card_no_denghao,
			                      List<Column_Card> list_card_order) 
	{
		String str="";
		if(list_card_denghao.size()==0 && list_card_no_denghao.size()==0 && list_card_order.size()==0)
		{
			return str;
		}

		//依次把三个排序数组串起来即可
		//等值条件字段放在整个索引字段的第一阵营
		for(int i=0;i<list_card_denghao.size();i++)
		{
			str=str+","+list_card_denghao.get(i).column_name;
		}
		
		//排序字段放在整个索引字段的第二阵营
		for(int k=list_card_order.size()-1;k>=0;k--)
		{
			int addr=str.indexOf(","+list_card_order.get(k).column_name);
			if(addr<0){
				str=str+","+list_card_order.get(k).column_name;
			}
		}
		
	    //非等值条件字段放在整个索引字段的第三阵营
		for(int j=0;j<list_card_no_denghao.size();j++)
		{
			if(str.indexOf(","+list_card_no_denghao.get(j).column_name) == -1)
			{
			     str=str+","+list_card_no_denghao.get(j).column_name;
			}
		}
		
		if(str.substring(0, 1).equals(",")==true)
			str=str.substring(1);
		
		return str;
	}

	/*
	 * 对列的Cardinality进行排序
	 */
	private void SortColumnCard(List<Column_Card> list_card,
			AnalyzeTableStructure ats,int type) {
		// TODO Auto-generated method stub
		AnalyzeColumnStructure acs;
		if(ats.list.isEmpty()==true) return;
		//装载数据
		//等号操作
		if(type==0)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    //不计算排序字段的Cardinality
			    if(acs.is_order_column==true) continue;
			    if(acs.symbol.equals("=")==true)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
		}
		
		//非等号操作
		if(type==1)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    //不计算排序字段的Cardinality
			    if(acs.is_order_column==true) continue;
			    if(acs.symbol.equals("=")==false)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
		}
		
		//排序字段操作
		if(type==2)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    if(acs.is_order_column==true)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
			
			//排序字段的话,不需要比较Cardinality,直接返回
			return;
		}
		
		//排序
		for(int i=0;i<list_card.size();i++)
		{
			for(int j=i+1;j<list_card.size();j++)
			{
				if(list_card.get(i).Cardinality < list_card.get(j).Cardinality)
				{
					//交换
					Column_Card tmp_cc = list_card.get(i);
					list_card.set(i, list_card.get(j));
					list_card.set(j, tmp_cc);	
				}
			}
		}
	}

	
	/*
	 * 对列的Cardinality进行排序
	 */
	private void SortColumnCard(List<Column_Card> list_card,AnalyzeTableStructure ats,
			int type,Table_Relationship table_Relationship) 
	{
		AnalyzeColumnStructure acs;
		if(ats.list.isEmpty()==true) return;
		//装载数据
		//等号操作
		if(type==0)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    //不计算排序字段的Cardinality
			    if(acs.is_order_column==true) continue;
			    //连接字段columnname2不能作为条件
				if(acs.is_join_key==1 && acs.column_name.equals(table_Relationship.columnname2)==true){
					continue;
				}
			    if(acs.symbol.equals("=")==true)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
		}
		
		//非等号操作
		if(type==1)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    //不计算排序字段的Cardinality
			    if(acs.is_order_column==true) continue;
			    if(acs.symbol.equals("=")==false)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
		}
		
		//排序字段操作
		if(type==2)
		{
			for(Iterator<AnalyzeColumnStructure> r = ats.list.iterator();r.hasNext();)
			{
			    acs=r.next();
			    if(acs.is_order_column==true)
			    {
				    Column_Card cc = new Column_Card();
				    cc.column_name=acs.column_name;
				    cc.Cardinality=acs.Cardinality;
				    cc.acs=acs;
				    list_card.add(cc);
			    }
			}
			
			//排序字段的话,不需要比较Cardinality,直接返回
			return;
		}
		
		//排序
		for(int i=0;i<list_card.size();i++)
		{
			for(int j=i+1;j<list_card.size();j++)
			{
				if(list_card.get(i).Cardinality < list_card.get(j).Cardinality)
				{
					//交换
					Column_Card tmp_cc = list_card.get(i);
					list_card.set(i, list_card.get(j));
					list_card.set(j, tmp_cc);	
				}
			}
		}
	}

	/*
	 * 装载排序数据
	 */
	private void LoadOrderDataToACS(String orderbycolumn,
			                        AnalyzeTableStructure ats,Map<String, String> map) {
		
		//如果没有排序字段,不需要后面的操作
		if(orderbycolumn.length()==0) return;
		//使用集合主要是为了去重
		Set<String> set = new HashSet<String>();
		String[] tmp = orderbycolumn.split(",");
		for(int i=0;i<tmp.length;i++)
		{
			set.add(tmp[i]);
		}
		
		String column_name;
		for(Iterator<String> r = set.iterator();r.hasNext();)
		{
			column_name=r.next();
			//要先检查在ats里列是否已存在,如果不存在,存入,如果存在,直接更改字段
			checkOrderColumnExist(ats,column_name,map);
		}
	}

	/*
	 * 因为我们是先装载where条件字段数据,再装载排序字段,所以可能会存在重复
	 */
	private void checkOrderColumnExist(AnalyzeTableStructure ats, String column_name,Map<String, String> map) {
		// TODO Auto-generated method stub
		AnalyzeColumnStructure acs;
		boolean is_find=false;
		for(Iterator<AnalyzeColumnStructure> r=ats.list.iterator();r.hasNext();)
		{
			acs=r.next();
			if(acs.column_name.equals(column_name)==true)
			{
				//存在,则只需要更改排序字段的值
				acs.is_order_column=true;
				is_find=true;
				break;
			}
		}
		
		if(is_find==false)
		{
			//直接写入ats中
			AnalyzeColumnStructure newacs = new AnalyzeColumnStructure();
			newacs.column_name=column_name;
			//查找这一列的元数据
			Column_Node cn=md.searchColumnMetaData(ats.tablename, column_name);
			if(cn==null){
				//看看是否使用了别名
				if(map==null){
					auto_review_error="Table:"+ats.tablename+" Column:"+column_name+" does not exist.";
					logger.warn(auto_review_error);
					return;
				}
				String real_column_name=map.get(column_name);
				if(real_column_name!=null){
					if(real_column_name.indexOf("(")>=0)
					{
						//使用函数这种也不处理了
						return;
					}else {
						cn=md.searchColumnMetaData(ats.tablename, real_column_name);
						if(cn==null){
							auto_review_error="Table:"+ats.tablename+" Column:"+column_name+" does not exist.";
							logger.warn(auto_review_error);
							return;
						}else {
							newacs.column_name=real_column_name;
						}
					}
				}else {
					auto_review_error="Table:"+ats.tablename+" Column:"+column_name+" does not exist.";
					logger.warn(auto_review_error);
					return;
				}
				
			}
			
			newacs.column_type=cn.column_type;
			newacs.is_order_column=true;
			ats.list.add(newacs);
		}
	}

	/*
	 * 装裁where数据
	 */
	private void LoadWhereDataToACS(Tree_Node whereNode,
			                        AnalyzeTableStructure ats) {
		// TODO Auto-generated method stub
		if(whereNode==null)
		{
			logger.warn("LoadWhereDataToACS装裁where数据时出现异常,whereNode=null");
			return;
		}
		if(whereNode.node_type==4)
		{
			//当前结点为and / or
			LoadWhereDataToACS(whereNode.left_node,ats);
			LoadWhereDataToACS(whereNode.right_node,ats);
		}
		else if(whereNode.node_type==2)
		{
			AnalyzeColumnStructure acs = new AnalyzeColumnStructure();
			//当前结点为操作符>,<,=,>=,<= in
			acs.symbol=whereNode.node_content;
			//计算操作符分值,等号分值最高
			if(acs.symbol.equals("="))
				acs.symbol_score = 10;
			else {
				acs.symbol_score = 5;
			}
			//左孩子肯定为条件变量列名
			acs.column_name=whereNode.left_node.node_content;
			//查找这一列的元数据
			Column_Node cn=md.searchColumnMetaData(ats.tablename, acs.column_name);
			if(cn==null){
				auto_review_error="Table:"+ats.tablename+" Column:"+acs.column_name+" does not exist.";
				logger.warn(auto_review_error);
				return;
			}
			acs.column_type=cn.column_type;
			//to do
			acs.column_type_score=100;
			acs.is_null_able=cn.is_nullable;
			acs.Cardinality=cn.sample_card;
			//to do 这里的分数是一个相对的分数
			acs.Cardinality_score=100;
		    //查找这个列所在的索引元数据
			acs.list_index = md.searchIndexMetaData(ats.tablename, acs.column_name);
			if(acs.list_index.isEmpty()==true)
				acs.exist_index=false;
			else {
				acs.exist_index=true;
			}
			acs.type=1;
			
			//将列的所有信息添加到table中,准备后续的计算使用
			if(!checkExistAcsInAts(acs,ats)){
			    ats.list.add(acs);
			}
		}
	}

	//打印所有的SQL
	public void printAllSQL()
	{
		String sql;
		logger.info("所有待审核的SQL总共"+list_sql.size()+"条，SQL如下：");
		for(Iterator<SQL_Node> r=list_sql.iterator();r.hasNext();)
    	{
    		sql = r.next().sqlString;
    		logger.info("SQL="+sql);
    	}
	}
	
	/*
	 * 前端调用的接口
	 */
	public void createIndexService(int fileMapId) throws Exception
	{
		    try {
				getAllSQL(fileMapId);
				reviewSQL();
			} catch (Exception e) {
				logger.error(e);
				throw e;
			}
	        
	}
	
	/**
	 * @param args
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws SQLException 
	{
        CreateIndex ci = new CreateIndex();
        //任何地方new这个对象，都需要对两个连接做一个检查，看看有没有创建成功，有任何一个没有创建成功，即刻返回
        if(ci.wtb.checkConnection()==false)
        {
        	logger.error("无法连接上SQL REVIEW DATABASE，请检查配置。");
        	return;
        }
        if(ci.md.checkConnection()==false)
        {
        	logger.error("无法连接上对应的 DATABASE，请检查配置。");
        	return;
        }
        
        ci.getAllSQL();
        ci.reviewSQL();
	}

}
