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

//保存表与表之间的关系,只保存表名与关联键
class Table_Relationship
{
	//表名
	String tablename;
	//表的别名
	String alias_tablename;
	//连接上一个表的关联键
	String columnname1;
	//连接下一个表的关联键
	String columnname2;
	//在此查询条件下，表的Card
	int Cardinality;
	
	public Table_Relationship()
	{
		tablename="";
		alias_tablename="";
		columnname1="";
		columnname2="";
		Cardinality=0;
	}
}
