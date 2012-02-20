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

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.regex.*;

import org.apache.log4j.Logger;



/*
 * function:只处理单表的parse
 */

public class ParseSQL {
	 //log4j日志
    private static Logger logger = Logger.getLogger(ParseSQL.class);
	
	//原始的SQL语句
	public String sql;
	//SQL类型 insert 0,update 1,delete 2,普通 select 3
	public int sql_type;
	//错误信息,多个错误信息,以:分隔
	public String errmsg;
	//建议
    public String tip;
	//表名
	public String tablename;
	//表别名
	public String alias_tablename;
	//where条件根结点
	public Tree_Node whereNode;
	//查询字段
    public String select_column;
    //查询别名与字段名的对应的hash map
    public Map<String, String> map_columns;
    //group by字段
    public String groupbycolumn;
    //排序字段
    public String orderbycolumn;
    //Muti-table sql statement tag
    public int tag;
	
	//构造函数
	public ParseSQL(String sql)
	{
		this.sql=sql.trim().toLowerCase();
		this.errmsg="";
		this.tip="";
		this.tablename="";
		this.groupbycolumn="";
		this.orderbycolumn="";
		//默认情况下都是单表查询0:单表;1:多表
		this.tag=0;
		this.map_columns=new HashMap<String, String>();
	}
	
	//分析一个SQL的类型select,insert,update,delete
	public void sql_dispatch()
	{
		
		if(sql.substring(0, 6).equals("select")==true)
		{
			//处理select
			parseSQLSelect();
			sql_type=3;
		}
		else if(sql.substring(0, 6).equals("insert")==true)
		{
		    //处理insert
			parseSQLInsert();
			sql_type=0;
		}
		else if(sql.substring(0,6).equals("update")==true)
		{
			//处理update
			parseSQLUpdate();
			sql_type=1;
		}
		else if(sql.substring(0, 6).equals("delete")==true)
		{
			//处理delete
			parseSQLDelete();
			sql_type=2;
		}
		else
			//非正常SQL语句
			sql_type=-1;
	}

	/*
	 * where条件生成的树的根结点,不能是or
	 * 如果确实存在这样的情况,需要DBA介入
	 */
	private void checkWhereTreeRootNode(Tree_Node treeRootNode)
	{
		if(treeRootNode==null)
		{
			this.errmsg="where tree root node is empty.";
			logger.warn(this.errmsg);
			return;
		}
		
		if(treeRootNode.node_content.equals("or")==true)
		{
			this.errmsg="where tree root node appears or key word,this is not allowed.";
			logger.error(this.errmsg);
		}
	}
	
	private void parseSQLDelete() {
		// TODO Auto-generated method stub
		// delete语句对于SQL auto review,只需要分析where条件字段即可
		logger.info("SQL at parsing:"+this.sql);
		int i=0;
		int loop=0;
		//第一次移动下标
		if(i+6<sql.length() && sql.substring(0, 6).equals("delete")==true) 
			i=i+6;
		else
		{
			this.errmsg="not delete SQL statement.";
			return;
		}
		
		//第二次移动下标,处理空格
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		
		//第三次移动下标,检测from
		if(i+4<sql.length() && sql.substring(i, i+4).equals("from")==true)
			i=i+4;
		else
		{
			this.errmsg="not find from key word.";
			return;
		}
		
		//第四次移动下标,处理空格
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		
		//第五次移动下标,检测tablename
		while(i+1<sql.length() && sql.substring(i, i+1).equals(" ")==false)
		{
			tablename=tablename+sql.substring(i, i+1);
			i++;
		}
		
		logger.info("table name:"+this.tablename);
		
		//第六次移动下标,处理空格
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		
		//第七次移动下标,检测where字段
		if(i+5<=sql.length() && sql.substring(i, i+5).equals("where")==true)
			i=i+5;
		else
		{
			this.errmsg="not find where key word.";
			return;
		}
		
		//异常处理
		if(i>sql.length()) {
			this.errmsg="not find where condition.";
			logger.warn(this.errmsg);
			return;
		}
		else
		{
			if(sql.substring(i).trim().length()==0)
			{
				this.errmsg="not find where condition.";
				logger.warn(this.errmsg);
				return;
			}
		}
		
		//处理后面的条件字段
		whereNode = parseWhere(null,sql.substring(i).trim(),loop);
		
		//check whereNode tree
		checkWhereTreeRootNode(whereNode);
		
		logger.info("where condition:"+sql.substring(i));
	}

	private void parseSQLUpdate() {
		// TODO Auto-generated method stub
		// update语句对于SQL auto review,只需要分析出tablename,以及where条件字段即可
		// update语句可能存在与select嵌套的情况
		logger.info("SQL at parsing:"+this.sql);
		int addr_where=0;
		int loop=0;
		tablename="";
		int i=0;
		
		//第一次移动下标
		if(i+6<sql.length() && sql.substring(0, 6).equals("update")==true) 
			i=i+6;
		else
		{
			this.errmsg="not update SQL statement.";
			return;
		}
		
		//第二次移动下标,处理空格
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		
		//第三次移动下标,检测tablename
		while(i+1<sql.length() && sql.substring(i, i+1).equals(" ")==false)
		{
			tablename=tablename+sql.substring(i, i+1);
			i++;
		}
		
		logger.info("table name:"+this.tablename);
		
		//check where key word
		addr_where=sql.indexOf(" where");
		if(addr_where<0)
		{
			this.errmsg="not find where key word.";
			logger.warn(this.errmsg);
			return;
		}
		
		//检查一下在values中是否有用sysdate()函数,这个函数会造成主备不一致
		if(sql.indexOf("sysdate()",i)>0 && sql.indexOf("sysdate()",i)<addr_where){
			errmsg="use sysdate() function,this not allowed,you should use now() replace it.";
			logger.warn(errmsg);
			return;
		}
		
		if(addr_where+6>=sql.length())
		{
			this.errmsg="not find where condition.";
			logger.warn(this.errmsg);
			return;
		}
		whereNode=parseWhere(null, sql.substring(addr_where+6), loop);
		
		//check whereNode tree
		checkWhereTreeRootNode(whereNode);
		
		logger.info("where condition:"+sql.substring(addr_where+5));
	}

	/*
	 * 检查查询字段是否符合规则
	 */
	private void selectColumnCheckValid(String columnsString) 
	{
		if(columnsString.equals("*")==true)
			tip="出现了select *,这是不倡导的,请写出明确的column name.";
	}
	
	/*
	 * 取得下一个单词,遇到单词后的空格后停止
	 */
    public String getNextToken(String str,int from_addr)
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
	 * 对各种当前支持的select SQL语句起到一个分发作用
	 */
	private void parseSQLSelect()
	{
		//where word check
		if(sql.indexOf(" where ")<0){
			this.errmsg="sql 不含有where关键字,需要DBA介入";
			return;
		}
		//and word check
		if(getNextToken(sql, sql.indexOf(" where ")+7).equals("and"))
		{
			this.errmsg="where条件第一个出现了and关键词,请检查sqlmapfile";
			return;
		}
		//&gt; &lt;这是SQLMAP中常见的一种错误
		if(sql.indexOf("&gt;")>0 || sql.indexOf("&lt;")>0){
			this.errmsg="in sql-map file,sql where语句中的>,<符号需要用<![CDATA[]]>转化";
			return;
		}
		
		//join关联方式暂不支持
		if(sql.indexOf(" join ")>0 && sql.indexOf(" on ")>0)
		{
			this.errmsg="join or left join or right join is not supported now.";
			return;
		}
		
		//最简单的SQL语句
		if(sql.indexOf(".") < 0)
		{
			//不能含有in子查询,这种会有多个select出现
			if(sql.indexOf("select ", 7)>0)
			{
				this.errmsg="sub-query is not supported now.";
				return;
			}
			parseSQLSelectBase();
			return;
		}
		
		//分页语句的特征
		if(sql.indexOf("order by") > 0 && sql.indexOf("limit ") > 0)
		{
			//再检查limit #start#,#end#语法
			int addr=sql.indexOf("limit ");
			addr=addr+6;
			if(sql.indexOf(",", addr) > sql.indexOf("limit "))
			{
				parseSQLSelectPage();
				return;
			}
		}
		
		//使用了.,并且存在多个select,现在也不支持
		if(sql.indexOf("select ", 7)>0)
		{
			this.errmsg="此种类型的多个select的查询暂不支持 .";
			return;
		}
		
		//看看是不是单表查询使用了别名
		int is_mutiple_table=checkMutipleTable(sql);
		//单表使用了别名
		if(is_mutiple_table==1){
			parseSQLSelectBase();
			return;
		}
		//多表关联
		if(is_mutiple_table==2){
			this.tag=1;
			return;
		}
		
		this.errmsg="当前SQL形式,还不支持";
		return;
	}
	
	//单表查询是否使用了别名
	//主要看from关键字后面
	//from tablename t where
	//返回值-1有错误
	//返回值0,单表未使用别名
	//返回值1,单表使用别名
	//返回值2,多表
	private int checkMutipleTable(String sql) {
		// TODO Auto-generated method stub
		int addr;
		int length=sql.length();
		int i;
		int start;
		boolean is_find_as=false;
		String alias_name="";
		addr=sql.indexOf(" from ");
		if(addr<0) return -1;
		i=addr+6;
		//space
		while(i+1<length && sql.substring(i, i+1).equals(" ")==true)
			i++;
		//table name
		while(i+1<length && sql.substring(i, i+1).equals(" ")==false)
			i++;
		//space
		while(i+1<length && sql.substring(i, i+1).equals(" ")==true)
			i++;
		//检查有没有tablename as t1这种特殊语法
		if(i+3<sql.length() && sql.substring(i, i+3).equals("as ")==true){
			i=i+3;
			is_find_as=true;
		}
		//token=where?
		start=i;
		while(i+1<length 
				&& sql.substring(i, i+1).equals(" ")==false
				&& sql.substring(i,i+1).equals(",")==false)
			i++;
		alias_name=sql.substring(start, i).trim();
		if(alias_name.equals("where")==true){
			return 0;
		}
		else 
		{
			//判断第一个非空字符是否为,号
			while(i+1<length && sql.substring(i, i+1).equals(" ")==true)
				i++;
			if(sql.substring(i, i+1).equals(",")==true)
			{
				logger.info("mutiple tables,this is not support now.");
				return 2;
			}
			//单表使用了别名,则需要进行替换
			logger.info("alias name:"+alias_name);
			this.sql=this.sql.replace(" "+alias_name+" ", " ");
			alias_name=alias_name+".";
			this.sql=this.sql.replace(alias_name, "");
			if(is_find_as==true){
				this.sql=this.sql.replace(" as ", " ");
			}
			
			return 1;
		}
		
	}

	/*
	 * select column_name,[column_name] from table_name where 条件 order by column_name limit #endnum
	 * 处理最简单的select SQL查询
	 */
	private void parseSQLSelectBase() {
		// TODO Auto-generated method stub
		int i=0,tmp=0;
		int addr_from;
		int addr_where;
		int addr_group_by;
		int addr_order_by;
		int addr_limit;
		String wherestr="";
		int loop=0;
		
		logger.info("SQL at parsing:"+sql);
		
		// 检查select关键字
		if(i+6<sql.length() && sql.substring(0, 6).equals("select")==true)
		{
			i=i+6;
		}
		else
		{
			this.errmsg="not select SQL statement.";
			return;
		}
		
		//处理查询字段，并检查合法性
		addr_from=sql.indexOf(" from ");
		if(addr_from==-1)
		{
			this.errmsg="not find from key word.";
			return;
		}
		this.select_column=sql.substring(i, addr_from).trim();
		selectColumnCheckValid(this.select_column);
		//处理列名的别名映射
		addToColumnHashMap(this.select_column,this.map_columns);
		
		logger.info("select columns:"+this.select_column);
		
		//处理table name
		i=addr_from+6;
		addr_where=sql.indexOf(" where ", i);
		if(addr_where==-1)
		{
			this.errmsg="Select SQL语句中必须包含where关键字,如果这条SQL确实需要,需要DBA介入审核";
			return;
		}
		
		this.tablename=sql.substring(i, addr_where);
		
		logger.info("table name:"+this.tablename);
		
		//处理where条件
		i=addr_where+7;
		addr_group_by = sql.indexOf("group by");
		addr_order_by = sql.indexOf("order by");
		addr_limit = sql.indexOf("limit ");
		
		if(addr_group_by<0 && addr_order_by<0 && addr_limit<0)
		{
			wherestr = sql.substring(i);
		}
		else {
			for(tmp=i;tmp<sql.length()-8;tmp++)
			{
				if(sql.substring(tmp, tmp+8).equals("group by")== false 
						&& sql.substring(tmp,tmp+8).equals("order by")==false 
						&& sql.substring(tmp, tmp+6).equals("limit ")==false)
				wherestr=wherestr+sql.substring(tmp, tmp+1);
				else {
					break;
				}
			}
		}
		//处理where string
		int wherestr_len=wherestr.length();
		wherestr=handleBetweenAnd(wherestr);
		this.whereNode=this.parseWhere(null,wherestr,loop);
		
		//check whereNode tree
		checkWhereTreeRootNode(whereNode);
		
		logger.info("where condition:"+wherestr);
		
		//继续处理排序,只能加上handleBetweenAnd函数处理之前的wherestr的长度
		i=i+wherestr_len;
		if(i<sql.length())
		{
			if(sql.substring(i, i+8).equals("group by")==true)
			{
				//解析后面的排序字段,也包括order by的字段,碰到语句结束
				//有group by的时候,后面通常有having关键字
				if(sql.indexOf("having", i+8) > 0)
				{
					this.groupbycolumn=sql.substring(i+8, sql.indexOf("having", i+7)).trim();
				}
				else if(sql.indexOf("order by", i+8)>0)
				{
					this.groupbycolumn=sql.substring(i+8, sql.indexOf("order by", i+8)).trim();
					
				}
				else if(sql.indexOf("limit",i+8)>0){
					this.groupbycolumn=sql.substring(i+8, sql.indexOf("limit", i+8)).trim();
				}
			}
		    
			logger.info("group by columns:"+this.groupbycolumn);
			
			if(sql.indexOf("order by",i) >= i)
			{
				if(sql.indexOf("limit ",i) > sql.indexOf("order by",i))
				{
					//含有limit,解析后面的排序字段,遇到limit终止
					if(this.orderbycolumn.length()>0)
						this.orderbycolumn=this.orderbycolumn+","+sql.substring(sql.indexOf("order by")+8, sql.indexOf("limit"));
					else {
						this.orderbycolumn=sql.substring(sql.indexOf("order by",i)+8, sql.indexOf("limit "));
					}	
				}
				else {
					//不含有limit,则直接到末尾
					if(this.orderbycolumn.length()>0)
						this.orderbycolumn=this.orderbycolumn+","+sql.substring(sql.indexOf("order by",i)+8);
					else {
						this.orderbycolumn=sql.substring(sql.indexOf("order by",i)+8);
					}
				}
				
				this.orderbycolumn=this.orderbycolumn.replace(" asc", " ");
				this.orderbycolumn=this.orderbycolumn.replace(" desc", " ");
			}
			
			this.orderbycolumn=this.orderbycolumn.replace(" ", "");
			logger.info("order by columns:"+this.orderbycolumn);
		}
	}

	/*
	 * 处理列名的别名映射
	 * column as alias_column,column as alias_column
	 * or
	 * function(column) as alias_column
	 * 示例:
	 * SELECT CONCAT(last_name,', ',first_name) AS full_name FROM mytable ORDER BY full_name;
	 * 在为select_expr给定别名时，AS关键词是自选的。前面的例子可以这样编写：
     * SELECT CONCAT(last_name,', ',first_name) full_name FROM mytable ORDER BY full_name;
	 */
	public static void addToColumnHashMap(String select_exprs,
			Map<String, String> map) 
	{
		//参数判断
		if(select_exprs==null){
			return;
		}
		select_exprs=select_exprs.toLowerCase();
		logger.debug("addToColumnHashMap select_exprs:"+select_exprs);
		//先处理最简单的情况
		if(select_exprs.indexOf("(")<0)
		{
			String[] array_columns=select_exprs.split(",");
			for(int i=0;i<array_columns.length;i++)
			{
				dealSingleSelectExpr(array_columns[i],map);
			}
			return;
		}
		
		//使用了函数,处理有括号的情况,括号
		int i=0;
		int start=0;
		int addr_douhao=0;
		int douhao_before_left_kuohao;
		int douhao_before_right_kuohao;
		String select_expr;
		while(i<select_exprs.length())
		{
			addr_douhao=select_exprs.indexOf(",",i);
			if(addr_douhao<0){
				//最后一组select_expr
				select_expr=select_exprs.substring(start);
		    	dealSingleSelectExpr(select_expr, map);
				break;
			}
			//检查这个逗号是否是正确的逗号,而不是函数里所使用的逗号
			douhao_before_left_kuohao=getWordCountInStr(select_exprs,"(",addr_douhao);
			douhao_before_right_kuohao=getWordCountInStr(select_exprs,")",addr_douhao);
		    if(douhao_before_left_kuohao==douhao_before_right_kuohao){
		    	//这是一个完整的select_expr
		    	select_expr=select_exprs.substring(start, addr_douhao);
		    	dealSingleSelectExpr(select_expr, map);
		    	start=addr_douhao+1;
		    	i=start;
		    }else {
				//这是函数里面的,寻找下一个逗号
		    	i=addr_douhao+1;
			}
		}
	}
	
	/*
	 * 统计一个符号出现的次数
	 */
	private static int getWordCountInStr(String str,String symbol,int addr_douhao)
	{
		int count=0;
		if(str==null || symbol==null ||str.length()<=addr_douhao){
			return -1;
		}
		for(int i=0;i<addr_douhao;i++)
		{
			if(str.substring(i, i+1).equals(symbol)){
				count++;
			}
		}
	
		return count;
	}
	
	/*
	 * 处理单个的select_expr
	 * column as alias_column
	 * or
	 * function(column) as alias_column
	 */
	private static void dealSingleSelectExpr(String select_expr,Map<String, String> map)
	{
		String alias_column_name="";
		String column_name="";
		String word="";
		
		
		if(select_expr==null || select_expr.trim().equals("")){
			return;
		}
		
		logger.debug("dealSingleSelectExpr select_expr:"+select_expr);
		
		int k=select_expr.length();
		//获得别名
		while(k-1>=0 && !select_expr.substring(k-1, k).equals(" "))
		{
			alias_column_name=select_expr.substring(k-1, k)+alias_column_name;
			k--;
		}
		if(k==0){
			//列名不含有别名
			column_name=alias_column_name;
			map.put(alias_column_name, column_name);
			logger.debug("column_name:"+column_name+" alias_column_name:"+alias_column_name);
		}
		//处理空格
		while(k-1>=0 && select_expr.substring(k-1, k).equals(" "))
		{
			k--;
		}
		//处理as,或者列真名或者函数名
		while(k-1>=0 && !select_expr.substring(k-1, k).equals(" "))
		{
			word=select_expr.substring(k-1, k)+word;
			k--;
		}
		
		if(!word.equals("as"))
		{
			column_name=word;
			logger.debug("column_name:"+column_name+" alias_column_name:"+alias_column_name);
			map.put(alias_column_name,column_name);
			return;
		}
		
		//处理空格
		while(k-1>=0 && select_expr.substring(k-1, k).equals(" "))
		{
			k--;
		}
		
		//列真名或者函数名
		column_name=select_expr.substring(0, k);
		logger.debug("column_name:"+column_name+" alias_column_name:"+alias_column_name);
		map.put(alias_column_name,column_name);	
	}

	/*
	 * 处理where语句中的between and 语法
	 * 
	 */
	public String handleBetweenAnd(String wherestr) 
	{
		String tmp_wherestr=wherestr;
		String resultString="";
		String column_name;
		int start=0;
		String matchString;
		int addr,len;
		
		if(tmp_wherestr.indexOf(" between ") < 0)
		{
			resultString = tmp_wherestr;
		}
		else 
		{
			//把between #value# and中间#value#中的空格要去掉
			tmp_wherestr=removeSpace(tmp_wherestr);
			Pattern pattern = Pattern.compile("\\s+[a-zA-Z][0-9_a-zA-Z\\.]+\\s+between\\s+[',:#+\\-0-9_a-zA-Z\\(\\)]+\\sand\\s+");
			Matcher matcher = pattern.matcher(tmp_wherestr);
			while(matcher.find())
			{
				matchString=matcher.group();
				len=matchString.length();
				addr=tmp_wherestr.indexOf(matchString);
				column_name = matchString.trim().substring(0, matchString.trim().indexOf(" "));
				//把between替换成>=符号
				matchString=matchString.replace(" between ", " >= ");
				//在and后面的空格处插入<=符号
				matchString=matchString+column_name+" <= ";
				//构造当前的resultString
				resultString=resultString+tmp_wherestr.substring(start,addr)+matchString;
				//计算下次开始的start位置
				start=addr+len;
			}//end while
			
			//补全后面的SQL
			if(start < tmp_wherestr.length())
			{
				resultString=resultString+tmp_wherestr.substring(start);
			}
			
		}
		
		return resultString;
	}

	
	/*
	 * 把between #value# and中间#value#中的空格要去掉,以#代替
	 */
	public String removeSpace(String tmp_wherestr) {
		String tmpString="";
		int addr_between=tmp_wherestr.indexOf(" between ");
		int addr_and;
		int start=0;
		while(addr_between > -1)
		{
			addr_and = tmp_wherestr.indexOf(" and ", addr_between);
			tmpString=tmpString+tmp_wherestr.substring(start, addr_between)+" between "+tmp_wherestr.substring(addr_between+9, addr_and).trim().replaceAll(" ", "#")+" and ";
			addr_between=tmp_wherestr.indexOf(" between ",addr_and+5);
			start= addr_and+5;
		}
		if(start<tmp_wherestr.length())
		{
			tmpString=tmpString+tmp_wherestr.substring(start);
		}
		return tmpString;	
	}

	/*
	 * 淘宝mysql单表分页的规范
	 * 按照分页的规则来匹配,处理单表分页的解析
	 * 示例1
	 * root@test 09:44:03>explain select t1.id, t1.manager_nick, t1.gmt_create, t1.nick
		-> from (select id
		-> from test
		-> where manager = ''
		-> and is_open = 2
		-> order by gmt_create limit 1, 10) t2 straight_join test t1
		-> where t1.id=t2.id
	 * 示例2
	 * root@test 09:45:28>explain select t1.id, t1.manager_nick, t1.gmt_create, t1.nick
		-> from (select id
		-> from test
		-> where manager = ''
		-> and is_open = 2
		-> order by gmt_create limit 1, 10) t2 ,test t1 force index(primary)
		-> where t1.id=t2.id;
	 */
	private void parseSQLSelectPage()
	{
		logger.info("SQL at parsing:"+this.sql);
		int i=0;
		int addr_from;
		String subsqlString;
		String alias_left_table;
		String alias_right_table;
		String real_tablename;
		if(i+6<sql.length() && sql.substring(0, 6).equals("select")==true)
		{
			i=i+6;
		}
		else
		{
			this.errmsg="not select SQL statement.";
			return;
		}
		addr_from=sql.indexOf(" from ");
		if(addr_from==-1)
		{
			this.errmsg="not find from key word.";
			return;
		}
		this.select_column=sql.substring(i, addr_from).trim();
		selectColumnCheckValid(this.select_column);
		if(this.errmsg.length()>0) return;
		
		i=addr_from+6;
		//处理from后面的括号内所有内容,括号内内容必须符合如下的格式
		//select primary key from table name where 条件 order by column_name asc/desc limit #1,#2
		//用stack来获取这个sub SQL查询
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		//必须找到左括号
		if(i+1<sql.length() && sql.substring(i, i+1).equals("(")==false)
		{
			this.errmsg="没有找到分页语句的左括号";
			return;
		}
		int start=i;
		Stack<String> stack = new Stack<String>();
		String tmp_s;
		while (i<sql.length()) {
			tmp_s=sql.substring(i, i+1);
			if(tmp_s.equals(")")==false)
				//将所有压栈
			    stack.push(tmp_s);
			else {
				//出栈,直到遇到左括号
				while(stack.pop().equals("(")==false)
				{
					;
				}
				//判断栈是否为空,为空,则已找到正确位置	
				if(stack.isEmpty()==true)
					break;
			}
			
			i++;
		}//end while
		subsqlString=sql.substring(start+1, i);
		
		
		//处理驱动表别名
		i++;
		if(sql.indexOf(",", i) > 0)
		{
		    alias_left_table = sql.substring(i, sql.indexOf(",", i)).trim();
		    //处理表的真名
		    i= sql.indexOf(",", i)+1;
		    while(sql.substring(i,i+1).equals(" ")==true)
				i++;
		    real_tablename = sql.substring(i, sql.indexOf(" ", i));
		    //处理表的别名
		    i= sql.indexOf(" ", i);
		    while(sql.substring(i,i+1).equals(" ")==true)
				i++;
		    alias_right_table = sql.substring(i, sql.indexOf(" ",i));
		    //必须含有force index(primary)关键字
		    //step 1: force
		    i=sql.indexOf(" ",i);
		    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
				i++;
		   
		    if(i+5<sql.length() && sql.substring(i, i+5).equals("force")==false)
		    {
		    	this.errmsg="not find force key word.";
		    	return;
		    }
		    //step 2:index
		    i=i+5;
		    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
				i++;
		    if(i+5<sql.length() && sql.substring(i, i+5).equals("index")==false)
		    {
		    	this.errmsg="not find force index key word.";
		    	return;
		    }
		    //step 3:(primary)
		    i=i+5;
		    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
				i++;
		    if(i+1<sql.length() && sql.substring(i, i+1).equals("(")==false)
		    {
		    	this.errmsg="not find force index( key word.";
		    	return;
		    }
		    i++;
		    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
				i++;
		    if(i+7<sql.length() && sql.substring(i, i+7).equals("primary")==false)
		    {
		    	this.errmsg="not find force index(primary key word.";
		    	return;
		    }
		    i=i+7;
		    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
				i++;
		    if(i+1<sql.length() && sql.substring(i, i+1).equals(")")==false)
		    {
		    	this.errmsg="not find force index(primary) key word.";
		    	return;
		    }
		    i++;
		}
		else {
			//处理另外一种连接方式straight_join 
			if(sql.indexOf("straight_join", i) > 0)
			{
				alias_left_table=sql.substring(i, sql.indexOf("straight_join ", i)).trim();
				i=sql.indexOf("straight_join ", i)+14;
				while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
						i++;
				real_tablename = sql.substring(i, sql.indexOf(" ", i)).trim();
			    //处理表的别名
				i=sql.indexOf(" ", i);
			    while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
					i++;
			    alias_right_table = sql.substring(i, sql.indexOf(" ",i)).trim();
			    i= sql.indexOf(" ",i);
			}
			else {
				this.errmsg ="cann't recongnize table join method.";
				return;
			}
		}
		
		//处理where
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		
		if(i+6<sql.length() && sql.substring(i, i+6).equals("where ")==false)
		{
			this.errmsg="not find where key word.";
	    	return;
		}
		//处理关联条件
		i=i+6;
		while(i+1<sql.length() && sql.substring(i,i+1).equals(" ")==true)
			i++;
		if(sql.indexOf("=", i)==-1)
		{
			this.errmsg="关联条件没有使用=号";
			return;
		}
		else {
			ParseSQL ps = new ParseSQL(subsqlString);
			ps.sql_dispatch();
			if(ps.tablename.equals(real_tablename)==false)
			{
				this.errmsg="单表分页,里外表表不一致";
				return;
			}
			String str1=alias_left_table+"."+ps.select_column;
			String str2=alias_right_table+"."+ps.select_column;
			if(sql.indexOf(str1,i)==-1)
			{
				this.errmsg=this.errmsg+":没有找到"+str1;
				return;
			}
			
			if(sql.indexOf(str2,i)==-1)
			{
				this.errmsg=this.errmsg+":没有找到"+str2;
				return;
			}	
			
			//如果程序走到这里,说明完全匹配分页的标准写法
			this.tablename=ps.tablename;
			this.whereNode=ps.whereNode;
			this.orderbycolumn=ps.orderbycolumn;
		}
		
	}
	
	private void parseSQLInsert() 
	{
		// insert SQL
		logger.info(sql);
		int i=0;
		int addr_values;
		// 检查insert关键字
		if(sql.substring(0, 6).equals("insert")==true)
		{
			i=i+6;
		}
		else
		{
			errmsg="非insert语句";
			return;
		}
		
		//接下来的关键字是into
		while(sql.substring(i, i+1).equals(" ")==true)
		{
			i++;
		}
		if(sql.substring(i, i+4).equals("into")==false)
		{
			errmsg="insert sql语句缺少into关键字,出现语法错误";
			return;
		}
		else {
			i=i+4;
		}
		
		//接下来处理表名
		while(sql.substring(i, i+1).equals(" ")==true)
		{
			i++;
		}
		while(sql.substring(i, i+1).equals(" ")==false && sql.substring(i, i+1).equals("(")==false)
		{
			tablename=tablename+sql.substring(i, i+1);
			i++;
		}
		logger.info(tablename);
		//(col1,col2)values(#col1#,#col2#)
		addr_values=sql.indexOf("values",i);
		if(addr_values<0){
			errmsg="not find values key word.";
			logger.warn(errmsg);
			return;
		}
		
		//检查有没有写列名,必须要明确写明列名,不能为空
		int kuohao_left=sql.indexOf("(",i);
		int kuohao_right=sql.indexOf(")",i);
		if(kuohao_left>=i && kuohao_right > kuohao_left && kuohao_right < addr_values){
			;
		}else {
			errmsg="between tablename and values key word,you must write columns clearly.";
			logger.warn(errmsg);
			return;
		}
		
		//检查一下在values中是否有用sysdate()函数,这个函数会造成主备不一致
		if(sql.indexOf("sysdate()",addr_values)>0){
			errmsg="use sysdate() function,this not allowed,you should use now() replace it.";
			logger.warn(errmsg);
			return;
		}
	}
	
	/*
	 * 这个函数把基本的操作,例如a=5 build成一棵树
	 * 被 parseBase()函数调用
	 */
	private Tree_Node buildTree(Tree_Node rootnode,String str,int addr,int offset)
	{
		Tree_Node node = new Tree_Node();
		Tree_Node left_child_node = new Tree_Node();
		Tree_Node right_child_node = new Tree_Node();
		
		//提取出运算符
		node.node_content=str.substring(addr, addr+offset).trim();
		node.node_type=2;
		node.parent_node=rootnode;
		node.left_node=left_child_node;
		node.right_node=right_child_node;
		//左孩子
		left_child_node.node_content=str.substring(0, addr).trim();
		left_child_node.node_type=1;
		left_child_node.parent_node=node;
		left_child_node.left_node=null;
		left_child_node.right_node=null;
		//右孩子
		right_child_node.node_content=str.substring(addr+offset).trim();
		right_child_node.node_type=3;
		right_child_node.parent_node=node;
		right_child_node.left_node=null;
		right_child_node.right_node=null;
		
		return node;
	}
	/*
	 * 处理最基本的运算,例如a=5  或者 a>#abc#
	 */
	private Tree_Node parseBase(Tree_Node rootnode,String str)
	{
		int addr;
		
		addr=str.indexOf(">=");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,2);
		}
		
		addr=str.indexOf("<=");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,2);
		}
		
		addr=str.indexOf(">");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,1);
		}
		
		addr=str.indexOf("<");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,1);
		}
		
		addr=str.indexOf("!=");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,2);
		}
		
		addr=str.indexOf("=");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,1);
		}
		
		addr=str.indexOf(" in ");
		if(addr > 0) 
		{
			//运算符为in,需要处理括号,这部份代码需要完善
			//这里可能含有子查询
			return buildTree(rootnode,str,addr,4);
		}
		
		addr=str.indexOf(" like ");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,6);
		}
		
		addr=str.indexOf(" is ");
		if(addr > 0) 
		{
			return buildTree(rootnode,str,addr,4);
		}
		
		return null;
	}
	
	public Tree_Node parseWhere(Tree_Node rootnode,String str_where,int loop)
	{
		//递归深度控制
		loop++;
		if(loop>10000) return null;
		
		String str=str_where.trim();
		Tree_Node node=new Tree_Node();
		int addr_and;
		int addr_or;
		//检查是否有左括号出现,将对括号内的表达式进行递归
		if(str.substring(0, 1).equals("(")==true){
			    //需找到跟它对称的右括号的位置
				//SQL语句中含有in关键字段,需要处理括号
				Stack<String> stack = new Stack<String>();
				int k=0;
				String tmp_s;
				while (k<str.length()) {
					tmp_s=str.substring(k, k+1);
					if(tmp_s.equals(")")==false)
						//将所有压栈
					    stack.push(tmp_s);
					else {
						//出栈,直到遇到左括号
						while(stack.pop().equals("(")==false)
						{
							;
						}
						//判断栈是否为空,为空,则已找到正确位置	
						if(stack.isEmpty()==true)
							break;
					}
					
					k++;
				}//end while
				
				if(k==str.length()-1)
				{
					//则右侧无表达式
					return parseWhere(rootnode,str.substring(1,k),loop);
				}
				else {
					//右侧有表达式,并找到第一个and 或者 or,至少有一个
					if(str.substring(k+1, k+6).equals(" and ")==true)
					{
						node.node_content="and";
						node.node_type=4;
						node.left_node=parseWhere(node, str.substring(1,k), loop);
						node.right_node=parseWhere(node, str.substring(k+6), loop);
						node.parent_node=rootnode;
					}
					else if(str.substring(k+1, k+5).equals(" or ")==true)
					{
						node.node_content="or";
						node.node_type=4;
						node.left_node=parseWhere(node, str.substring(1,k), loop);
						node.right_node=parseWhere(node, str.substring(k+5), loop);
						node.parent_node=rootnode;
					}
					
					return node;
				    
				}
		}
		else 
		{
			addr_and = str.indexOf(" and ");
			addr_or = str.indexOf(" or ");
			if(addr_and > 0 && addr_or > 0)
				if(addr_and < addr_or)
				{
					//最早找到and
					node.node_content="and";
			    	node.node_type=4;
			    	node.parent_node=rootnode;
			    	node.left_node=parseBase(node,str.substring(0,addr_and).trim());
			    	node.right_node=parseWhere(node,str.substring(addr_and+5),loop);
			    	return node;
				}
				else
				{
					//最早找到or
					node.node_content="or";
				    node.node_type=4;
				    node.parent_node=rootnode;
				    node.left_node=parseBase(node,str.substring(0,addr_or).trim());
				    node.right_node=parseWhere(node,str.substring(addr_or+4),loop);
				    return node;
				}
			else if(addr_and > 0)
			{
				node.node_content="and";
		    	node.node_type=4;
		    	node.parent_node=rootnode;
		    	node.left_node=parseBase(node,str.substring(0,addr_and).trim());
		    	node.right_node=parseWhere(node,str.substring(addr_and+5),loop);
		    	return node;
			}
			
			else if(addr_or > 0)
			{
				node.node_content="or";
			    node.node_type=4;
			    node.parent_node=rootnode;
			    node.left_node=parseBase(node,str.substring(0,addr_or).trim());
			    node.right_node=parseWhere(node,str.substring(addr_or+4),loop);
			    return node;
			}
			else {
				//处理基本运算
	    	    return parseBase(rootnode,str);
			}
		}   
	}
    
	/*
	 * 输出一颗树的信息
	 */
	public void printTree(Tree_Node rootnode)
	{
		if(rootnode != null)
		{	
			System.out.println("NODE ID:"+rootnode.hashCode()+", NODE CONTENT:"+rootnode.node_content);
		}
		
		if(rootnode.left_node != null)
		{
			System.out.println("My PARENT NODE CONTENT:"+rootnode.node_content+", NODE ID:"+rootnode.hashCode()+", LEFT CHILD ");
		    printTree(rootnode.left_node);
		}
		
		if(rootnode.right_node != null)
		{
			System.out.println("My PARENT NODE CONTENT:"+rootnode.node_content+", NODE ID:"+rootnode.hashCode()+", RIGHT CHILD ");
			printTree(rootnode.right_node);
		}
			
	}
	
}
