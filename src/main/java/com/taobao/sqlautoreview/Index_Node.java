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

public class Index_Node {
    //数据库名
	String table_schema;
	//表名
	public String table_name;
	//是否唯一
	int non_unique;
	//数据库名
	String index_schema;
	//索引的名字
	public String index_name;
	//索引字段在索引中的排列位置
	int seq_in_index;
	//列名
	String column_name;
	//列的势
	int Cardinality;
	//索引的类型,基本上都会是BTree
	String index_type;
}
