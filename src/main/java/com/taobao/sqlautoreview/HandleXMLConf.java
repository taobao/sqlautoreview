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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;


import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import org.apache.log4j.Logger;

/*
 * function:处理XML配置的一个类
 */
public class HandleXMLConf {
	Element root;
	//log4j日志
    private static Logger logger = Logger.getLogger(HandleXMLConf.class);
	
	public HandleXMLConf(String filename)
	{
        //String path = getClass().getResource("/").getPath();
		//String sqlmapfilename="sqlreviewdb.xml";
		//sqlmapfilename=sqlmapfilename.replaceAll("%20", " ");
		
		try{
            Document dom = loadXml(filename);
            root = dom.getRootElement();
            if(root == null){ 
            	logger.error("无法找到数据库配置文件的root根结点,程序退出");
            	return;
            }
		}
        catch(Exception e)
        {
        	logger.error("无法找到数据库的配置文件"+filename+",程序退出");
        	return;
        }
	}
	
	public Document loadXml(String path) throws DocumentException, IOException 
	{
		//InputStream input = new FileInputStream(Utils.getResourceAsFile(path));
		InputStream input=Utils.getResourceAsStream(path, "utf-8");
		SAXReader reader = new SAXReader();
		Document doc = reader.read(input);
		return doc;
	}
	
	public String getDbConfigIP()
    {
		String ip="";
        //读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	Element tmp=r.next();
        	if(tmp.getName().equals("ip"))
        	{
        		ip = tmp.getData().toString();
        	}
        }  
        return ip;
    }
	
	public int getDbConfigPort()
	{
		int port=-1;
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	Element tmp=r.next();
        	 if(tmp.getName().equals("port"))
        	 {
        		port = Integer.valueOf(tmp.getData().toString());
        		break;
        	 }
	    }
        return port;
	}
    
	public String getDbConfigDbname()
	{
		String dbname="";
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	Element tmp=r.next();
        	 if(tmp.getName().equals("dbname"))
        	 {
        		dbname = tmp.getData().toString();
        		break;
        	 }
	    }
        return dbname;
	}
	
	public String getDbConfigUser()
	{
		String user="";
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	Element tmp=r.next();
        	 if(tmp.getName().equals("user"))
        	 {
        		user = tmp.getData().toString();
        		break;
        	 }
	    }
        return user;
	}
	
	public String getDbConfigPassword()
	{
		String password="";
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	Element tmp=r.next();
        	 if(tmp.getName().equals("password"))
        	 {
        		password = tmp.getData().toString();
        		break;
        	 }
	    }
        return password;
	}
	
	public int getSQLMapFileID()
	{
		int file_id=-1;
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	 Element tmp=r.next();
        	 if(tmp.getName().equals("file_id"))
        	 {
        		 file_id = Integer.valueOf(tmp.getData().toString());
        		 break;
        	 }
	    }
        return file_id;
	}
	
	public String getSQLMapFileName()
	{
		String file_name="";
		//读取配置
        for(Iterator<Element> r = root.elementIterator();r.hasNext();)
        {
        	 Element tmp=r.next();
        	 if(tmp.getName().equals("file_name"))
        	 {
        		 file_name = tmp.getData().toString();
        		 break;
        	 }
	    }
        return file_name;
	}
}
