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

import java.util.List;

/*
 * interface类
 * HandleSQLReviewDB类是IHandleDB接口的一个实现
 */

public interface IHandleDB {
	//check connection status
	public boolean checkConnection();
	//将SQL写入到SQL review database中
	public boolean insertDB(int sqlmap_file_id, String java_class_id, String sql_xml, String real_sql, String sql_comment);
	//获得待审核的SQL,从配置文件中读取sqlmap_fileid
	public List<SQL_Node> getAllSQL();
	//获得待审核的SQL
	public List<SQL_Node> getAllSQL(int sqlmap_file_id);
	//更新这条SQL的审核状态
	public int updateSQLStatus(int status,String sql_auto_index,int id,String auto_review_err,String auto_review_tip);
	//更新这条SQL的审核状态
	public int updateSQLStatus(int status,String sql_auto_index,int id,String auto_review_err,String auto_review_tip,String tablenames);
	//获得审核出的所有index
	//merge index所用
	//这里只需要Index_Node中的table_name,index_name两个字段
	public List<Index_Node> getAllIndexes();
	//获得审核出的所有index
	//merge index所用
	//这里只需要Index_Node中的table_name,index_name两个字段
	public List<Index_Node> getAllIndexes(int sqlmap_file_id);
	//删除merge结果
	public void deleteMergeResult(int sqlmap_file_id);
	//插入merge结果
	public void saveMergeResult(int sqlmap_file_id,String tablename,String real_tablename,String exist_indexes,String new_indexes,String merge_result);
}

