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

public class Column_Node {
	//列名
	String column_name;
	//列的类型
	String data_type;
	//列的类型
	String column_type;
	//是否为空
	String is_nullable;
	//采样的势
	int sample_card;
}
