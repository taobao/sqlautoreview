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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;


import org.apache.log4j.Logger;



/*
 * 将一个table的索引进行合并
 */
public class TableMergeIndex 
{
	 //log4j日志
    private static Logger logger = Logger.getLogger(TableMergeIndex.class);
	//表名
	String tablename;
    //表上已经存在的索引
	List<MergeIndex_Node> list_exist_indexes;
	//本次SQL review新创建的索引
	List<MergeIndex_Node> list_new_indexes;
	//已存在的索引去重
	Set<MergeIndex_Node> set_exist_indexes;
	//新创建的索引去重
	Set<MergeIndex_Node> set_new_indexes;
	//结果
	Set<MergeIndex_Node> set_result_new_indexes;
	
	/*
	 * 构造函数
	 */
	public TableMergeIndex(String tablename)
	{
		this.tablename=tablename;
		this.list_exist_indexes=new LinkedList<MergeIndex_Node>();
		this.list_new_indexes=new LinkedList<MergeIndex_Node>();
		this.set_exist_indexes=new HashSet<MergeIndex_Node>();
		this.set_new_indexes=new HashSet<MergeIndex_Node>();
		this.set_result_new_indexes=new HashSet<MergeIndex_Node>();
	}
	
	/*
	 * 填冲两个list
	 */
	private void addIndexToList(List<MergeIndex_Node> list,int type)
	{
		if(list==null){
			return;
		}
		
		if(type==1){
			this.list_exist_indexes=list;
		}else if(type==2){
			this.list_new_indexes=list;
		}
	}
	
	/*
	 * index第一次简单自去重,通过set来完成
	 * index_name,indexed_columns两者都相同,才会去重
	 */
	private void deleteRepeatIndex1() 
	{
		if(this.list_exist_indexes.size()!=0){
			this.set_exist_indexes.addAll(list_exist_indexes);	
		}else {
			logger.info("list_exist_indexes size = 0.");
		}
		
		if(this.list_new_indexes.size()!=0){
			this.set_new_indexes.addAll(list_new_indexes);
		}else{
			logger.info("list_new_indexes size = 0.");
		}
	}
	
	/*
	 * index第二次去重
	 * 如果set_new_indexes中的index,已在set_exist_indexes存在,那么需要删除
	 * index_name,indexed_columns两者都相同,才会去重
	 */
	private void deleteRepeatIndex2() 
	{
		if(set_exist_indexes.size()==0 || set_new_indexes.size()==0){
			return;
		}
		
		for(MergeIndex_Node mergeIndex_Node:set_exist_indexes)
		{
			if(set_new_indexes.contains(mergeIndex_Node)){
				set_new_indexes.remove(mergeIndex_Node);
			}
		}
	}
	
	/*
	 * index第三次去重
	 * 具有相同的索引字段,索引名不同,这样的索引只需要保留一个即可
	 * 这种情况只存在于两个set之间,不可能存在于单个set内部
	 */
	private void deleteRepeatIndex3() 
	{
		String indexed_columns;
		if(set_exist_indexes.size()==0 || set_new_indexes.size()==0){
			return;
		}
		for(MergeIndex_Node mergeIndex_Node:set_exist_indexes)
		{
			indexed_columns=mergeIndex_Node.indexed_columns;
			//logger.info(indexed_columns);
			removeExistIndexColumns(indexed_columns);
		}
	}
	
	/*
	 * 删掉在set_new_indexes与老的索引字段相同的节点
	 */
	private void removeExistIndexColumns(String indexed_columns)
	{
		logger.debug("removeExistIndexColumns:"+set_new_indexes.size());
		Set<MergeIndex_Node> tmp_set_new_indexes=new HashSet<MergeIndex_Node>();
		
		if(set_new_indexes.size()==0){
			return;
		}
		
		for(MergeIndex_Node mergeIndex_Node:set_new_indexes)
		{
				tmp_set_new_indexes.add(mergeIndex_Node);
		}
		
		set_new_indexes.clear();
		
		for(MergeIndex_Node mergeIndex_Node:tmp_set_new_indexes){
			if(mergeIndex_Node.indexed_columns.equals(indexed_columns)==false)
			{
				set_new_indexes.add(mergeIndex_Node);
			}
		}
		tmp_set_new_indexes.clear();
		logger.debug("removeExistIndexColumns:"+set_new_indexes.size());
	}

	/*
	 * 新建的索引字段,是已存在的索引字段的子集,并且顺序要一样,这样的索引也可以删除
	 * set_new_indexes内部需要处理,set_new_indexes与set_exist_indexes也需要处理
	 */
	private void indexMerge1() {
		//优先处理set_new_indexes与set_exist_indexes之间的相似之处
		//以原来存在的索引为基准
		 compareNewIndexToExistIndex();
		
		//再处理set_new_indexes内部的相似之处
		//需要将set_new_indexes字段的个数多少先进行排序,字段少的索引合并到字段多的索引上
		 mergeNewIndexes();
		
		//看看原来存在的索引,是否有需要自合并的地方
		//如果有,需要把删除索引的脚本放入到set_result_new_indexes中
		//删除原有索引的动作要小心
		 mergeExistIndexes();
		
		
		//看看原来存在的索引,是不是新建索引,索引字段的子集
		//如果有,需要把删除索引的脚本放入到set_result_new_indexes中
		//删除原有索引的动作要小心
		 compareExistIndexToLastNewIndex();
	}
	
	/*
	 * 同一个表上的索引名不能相同
	 * 需要处理set_result_new_indexes的索引名
	 */
	private void indexRename()
	{   Random radom=new Random();
		for(MergeIndex_Node mergeIndex_Node:set_result_new_indexes)
		{
			//索引名下标
			int i=Math.abs(radom.nextInt())%100;
			String index_name=mergeIndex_Node.index_name;
			//这种要删掉的索引,是不需要处理的
			if(mergeIndex_Node.keep==-1){
				continue;
			}
			
			//看看在原来的索引中是否存在重名
			for(MergeIndex_Node tmp_mergeIndex_Node:set_exist_indexes){
				if(tmp_mergeIndex_Node.keep==-1){
					continue;
				}
				if(tmp_mergeIndex_Node.index_name.equals(index_name)){
					mergeIndex_Node.index_name=mergeIndex_Node.index_name+i;
					mergeIndex_Node.createIndexScript="create index "+mergeIndex_Node.index_name+" on "+this.tablename;
					mergeIndex_Node.createIndexScript=mergeIndex_Node.createIndexScript+"("+mergeIndex_Node.indexed_columns+")";
					break;
				}
			}
		}
	}
	/*
	 * 以原来存在的索引为基准,检查新的索引字段是否在原来的索引字段中
	 */
	private void compareNewIndexToExistIndex() 
	{
		String indexed_columns;
		for(MergeIndex_Node mergeIndex_Node:set_exist_indexes)
		{
			indexed_columns=mergeIndex_Node.indexed_columns;
			removeExistSimilarIndexColumns(indexed_columns);
		}
		
	}

	/*
	 * 对新的索引自行自合并
	 */
	private void mergeNewIndexes() 
	{
		if(set_new_indexes.size()==0){
			return;
		}
		
		List<MergeIndex_Node> list_sort=new LinkedList<MergeIndex_Node>();
		list_sort.addAll(set_new_indexes);
		sortMergeIndexNodeList(list_sort);
		list_sort=selfcheckMatch(list_sort);
		set_result_new_indexes.addAll(list_sort);	
	}

	/*
	 * 对原来存在的索引进行自合并
	 * drop index index_name on table
	 */
	private void mergeExistIndexes() 
	{
		if(set_exist_indexes.size()==0){
			return;
		}
		List<MergeIndex_Node> list_sort=new LinkedList<MergeIndex_Node>();
		list_sort.addAll(set_exist_indexes);
		sortMergeIndexNodeList(list_sort);
		list_sort=selfcheckMatch(list_sort);
		
		//将list_sort放入到一个新的集合中
		Set<MergeIndex_Node> set_exist_indexes2=new HashSet<MergeIndex_Node>();
		set_exist_indexes2.addAll(list_sort);
		
		//比较两个集合的差异,这个差异即是要删掉的索引
		for (Iterator<MergeIndex_Node> iterator=set_exist_indexes.iterator();iterator.hasNext();) {
			MergeIndex_Node mergeIndex_Node=iterator.next();
			if(!set_exist_indexes2.contains(mergeIndex_Node)){
				//打标记
				mergeIndex_Node.keep=-1;
				//不存在
				MergeIndex_Node tmp_merIndex_Node=new MergeIndex_Node(mergeIndex_Node.createIndexScript);
				tmp_merIndex_Node.createIndexScript="drop index "+mergeIndex_Node.index_name+" on "+this.tablename;
				tmp_merIndex_Node.keep=-1;
				set_result_new_indexes.add(tmp_merIndex_Node);
				logger.debug("mergeExistIndexes : drop exist index:"+tmp_merIndex_Node.createIndexScript);
			}
		}
		
	}

	
	/*
	 * 以最后要新建的索引为基准,检查以前存在的索引子段,是不是新建索引字段的子集
	 */
	private void compareExistIndexToLastNewIndex() 
	{
		String indexed_columns;
		for(MergeIndex_Node mergeIndex_Node:set_new_indexes)
		{
			indexed_columns=mergeIndex_Node.indexed_columns;
			String[] array_new_indexed_columns=indexed_columns.split(",");
			for(MergeIndex_Node mergeIndex_Node2:set_exist_indexes){
				//不需要处理已经删掉的索引
				if(mergeIndex_Node2.keep==-1){
					continue;
				}
				String[] array_exist_indexed_columns=mergeIndex_Node2.indexed_columns.split(",");
				if(checkMatch(array_new_indexed_columns,array_exist_indexed_columns)){
					//打标记
					mergeIndex_Node2.keep=-1;
					//添加进最后的结果当中
					MergeIndex_Node tmp_merIndex_Node=new MergeIndex_Node(mergeIndex_Node2.createIndexScript);
					tmp_merIndex_Node.createIndexScript="drop index "+mergeIndex_Node2.index_name+" on "+this.tablename;
					tmp_merIndex_Node.keep=-1;
					set_result_new_indexes.add(tmp_merIndex_Node);
					logger.debug("compareExistIndexToLastNewIndex : drop exist index:"+tmp_merIndex_Node.createIndexScript);
				}
			}
		}
		
		
		
	}
	
	/*
	 * 检查自己的索引字段,是否与自己的其它element有相似之处
	 */
	private List<MergeIndex_Node> selfcheckMatch(List<MergeIndex_Node> list_sort) 
	{
		//保存返回的结果
		List<MergeIndex_Node> tmp_list_sort=new LinkedList<MergeIndex_Node>();
		for(int i=0;i<list_sort.size();i++)
		{
			if(list_sort.get(i).keep==-1){
				continue;
			}
			String[] array_new_index1=list_sort.get(i).indexed_columns.split(",");
			for(int j=i+1;j<list_sort.size();j++)
			{
				String[] array_new_index2=list_sort.get(j).indexed_columns.split(",");
				if(checkMatch(array_new_index1,array_new_index2)){
					//先打标记
					list_sort.get(j).keep=-1;
				}
			}
		}
		for(int i=0;i<list_sort.size();i++){
			if(list_sort.get(i).keep!=-1){
				tmp_list_sort.add(list_sort.get(i));
			}
		}
		
		list_sort.clear();
		return tmp_list_sort;
		
	}

	/*
	 * 对一个list<MergeIndex_Node>按indexed_columns_num大小进行排序
	 */
	private void sortMergeIndexNodeList(List<MergeIndex_Node> list_sort) 
	{
		//排序
		for(int i=0;i<list_sort.size();i++)
		{
			for(int j=i+1;j<list_sort.size();j++)
			{
				if(list_sort.get(i).indexed_columns_num < list_sort.get(j).indexed_columns_num)
				{
					//交换
					MergeIndex_Node tmp_cc = list_sort.get(i);
					list_sort.set(i, list_sort.get(j));
					list_sort.set(j, tmp_cc);	
				}
			}
		}
		
	}

	/*
	 * 删掉在set_new_indexes与老的索引字段相似的节点
	 * 这里必须要按字段比较
	 */
	private void removeExistSimilarIndexColumns(String indexed_columns) 
	{
		String[] array_exist_indexed_columns=indexed_columns.split(",");
		Set<MergeIndex_Node> tmp_set_new_indexes=new HashSet<MergeIndex_Node>();
		tmp_set_new_indexes.addAll(set_new_indexes);
		for(Iterator<MergeIndex_Node> iterator=tmp_set_new_indexes.iterator();iterator.hasNext();)
		{
			MergeIndex_Node mergeIndex_Node=iterator.next();
			String[] array_new_indexed_columns=mergeIndex_Node.indexed_columns.split(",");
			if(checkMatch(array_exist_indexed_columns,array_new_indexed_columns)){
				set_new_indexes.remove(mergeIndex_Node);
			}
		}
	}

	
	/*
	 * 详细比较两个索引字段的相似之处
	 */
	private boolean checkMatch(String[] array_exist_indexed_columns,
			String[] array_new_indexed_columns) 
	{
		if(array_new_indexed_columns.length>array_exist_indexed_columns.length){
			return false;
		}
		for(int i=0;i<array_new_indexed_columns.length;i++){
			if(array_new_indexed_columns[i].equals(array_exist_indexed_columns[i])==false){
				return false;
			}
		}
		
		return true;
	}

	/*
	 * print合并后需要新建的索引
	 */
	public void print_result_new_index()
	{
		System.out.println("\n\n--------Merge Table "+this.tablename+" Indexes--------");
		System.out.println("exist index as follows:");
		for(MergeIndex_Node mergeIndex_Node:list_exist_indexes){
			System.out.println("  "+mergeIndex_Node.createIndexScript);
		}
		System.out.println("new index as follows:");
		for(MergeIndex_Node mergeIndex_Node:list_new_indexes){
			System.out.println("  "+mergeIndex_Node.createIndexScript);
		}
		System.out.println("Merge result:");
		if(set_result_new_indexes.size()==0){
			System.out.println("  不需要新建索引.");
			return;
		}
		for(MergeIndex_Node mergeIndex_Node:set_result_new_indexes)
		{
			System.out.println("  "+mergeIndex_Node.createIndexScript);
		}
	}
	
	private List<String> getMergeIndexResult()
	{
		List<String> list=new LinkedList<String>();
		for(MergeIndex_Node mergeIndex_Node:set_result_new_indexes)
		{
			list.add(mergeIndex_Node.createIndexScript);
		}
		return list;
	}
	/*
	 * 外部接口
	 */
	public List<String> tableMergeIndexService(List<MergeIndex_Node> list_exist_index,
			List<MergeIndex_Node> list_new_index)
	{
		addIndexToList(list_exist_index,1);
		addIndexToList(list_new_index,2);
		deleteRepeatIndex1();
		deleteRepeatIndex2();
		deleteRepeatIndex3();
		indexMerge1();
		indexRename();
		return getMergeIndexResult();
	}

}
