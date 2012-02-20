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

/*
 * 保存每个表分析后的结果
 */
class ParseStruct {
	//表名
	public String tablename;
	//表别名
	public String alias_tablename;
	//whereString
	public String whereString;
	//where条件根结点
	public Tree_Node whereNode;
	//查询字段
    public String select_column;
    //group by字段
    public String groupbycolumn;
    //排序字段
    public String orderbycolumn;
    
    public ParseStruct()
    {
    	groupbycolumn="";
    	orderbycolumn="";
    }
}
