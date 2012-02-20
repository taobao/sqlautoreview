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
 * 用于索引合并
 * 分析单条建索引的语句
 */
public class MergeIndex_Node {
	 //创建索引的完整脚本
	 String createIndexScript;
	 //索引的名字
     String index_name;
     //索引的字段
     String indexed_columns;
     //索引的字段个数
     int indexed_columns_num;
     //是否保留
     int keep;
     
     /*
      * 构造函数
      */
     public MergeIndex_Node(String createIndexScript)
     {
    	 this.createIndexScript=createIndexScript;
    	 this.index_name=getIndexName();
    	 this.indexed_columns=getIndexedColumns();
    	 this.indexed_columns_num=getIndexColumnsNum();
    	 this.keep=0;
     }
     
     /*
      * 获得索引的名字
      */
     private String getIndexName() 
     {
    	int addr_index=createIndexScript.indexOf(" index ");
    	int addr_on=createIndexScript.indexOf(" on ");
    	return createIndexScript.substring(addr_index+7, addr_on).trim();
	}
     
     /*
      * 获得索引的字段
      */
     private String getIndexedColumns() {
		int addr_left_kuohao=createIndexScript.indexOf("(");
		int addr_right_kuohao=createIndexScript.indexOf(")");
		return createIndexScript.substring(addr_left_kuohao+1, addr_right_kuohao).trim();
	}
     /*
 	 * 统计set_new_indexes各索引的索引字段个数
 	 */
 	private int getIndexColumnsNum() 
 	{
 		String[] array_indexed_columns=indexed_columns.split(",");
 		return array_indexed_columns.length;
 		
 		
 	}
}
