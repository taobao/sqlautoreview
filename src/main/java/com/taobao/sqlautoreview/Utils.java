/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 *
 * Authors:
 *   wuhua <wuhua@taobao.com>
 *
 */

package com.taobao.sqlautoreview;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;


public class Utils {

    public static Properties getResourceAsProperties(String resource, String encoding) throws IOException {
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream(resource);
        }
        catch (IOException e) {
            in = new FileInputStream(new File(resource));
        }

        Reader reader = new InputStreamReader(in, encoding);
        Properties props = new Properties();
        props.load(reader);
        in.close();
        reader.close();

        return props;

    }
    
    public static InputStream getResourceAsStream(String resource, String encoding) throws IOException {
        InputStream in = null;
        try {
            in = ResourceUtils.getResourceAsStream(resource);
        }
        catch (IOException e) {
            in = new FileInputStream(new File(resource));
        }

        

        return in;

    }


    public static File getResourceAsFile(String resource) throws IOException {
        try {
            return new File(ResourceUtils.getResourceURL(resource).getFile());
        }
        catch (IOException e) {
            return new File(resource);
        }
    }

    public abstract static class Action {
        public abstract void process(String line);


        public boolean isBreak() {
            return false;
        }
    }


    public static void processEachLine(String string, Action action) throws IOException {
        BufferedReader br = new BufferedReader(new StringReader(string));
        try {
            String line;
            while ((line = br.readLine()) != null) {
                action.process(line);
                if (action.isBreak()) {
                    break;
                }
            }

        }
        finally {
            br.close();
            br = null;
        }
    }


    

}
