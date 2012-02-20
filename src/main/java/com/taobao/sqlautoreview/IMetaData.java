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
 * 定义业务数据库访问接口,通过这些接口,可以获得相关元数据
 * author:danchen /zhaolin
 * create time:2012-1-13
 */
public interface IMetaData {
	//检查连接状态
	public boolean checkConnection();
	//构造单个表的index元数据
	public List<Index_Node> buildIndexMetaData(String tablename);
	//构造单个表的column元数据
    public List<Column_Node> buildColumnMetaData(String tablename);
    //构造一个表所需的元数据column+index of one table
    public void buildTableMetaData(String tablename);
    //构造这个库所有表的元数据
    public void buildDBMetaData();
    //根据表名,列名,获得指定列的元数据
    public Column_Node searchColumnMetaData(String tablename,String column_name);
    //根据表名,列名获得指定列上存在的索引的元数据,一个列可能已存在于多个索引当中,所以返回的是一个List
    public List<Index_Node> searchIndexMetaData(String tablename,String column_name);
    //检查表名在元数据Cache中是否存在
    public boolean checkTableExist(String tablename);
    /*
     * 检查表名在业务数据库中是否存在,有可能分库分表,格式tablename_XXXX
     * 使用了TDDL，很有可能会碰上这样的tablename
     * 这些tablename存在SQLMAP中，需要转化后才能使用
     * 如果实际的表名,与SQL中的表名不一致,需要让上层知道
     */
    public String findMatchTable(String tablename);
    //根据表名,获得表上的索引,这个是为前端展示服务的,后台程序不会用到这个接口
    //支持多表,参数tablenames传入规范tablename,tablename
    //后台的merge index也会调用这个函数
    //返回格式seller:PRIMARY(seller_id);idx_seller_nick(nick);|test1:PRIMARY(id);idx_up_uid(user_id,gmt_modified,is_delete);
    public String getIndexesByTableName(String tablenames);
    /*
	 * 不关闭连接,获取一个表的所有索引
	 */
	public String getIndexesByTableName2(String tablenames);
}
