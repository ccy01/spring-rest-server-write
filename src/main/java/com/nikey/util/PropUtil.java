package com.nikey.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Jayzeee
 * @created 2014-2-10 21:08:41
 * @description properties util
 */
public class PropUtil   
{  
      
	private static Properties config = getPropUtil();
	
    /**
     * @description init
     * @return properties
     */
    private static Properties getPropUtil () {  
          
    	config = new Properties();
        
        InputStream is = null;  
        try {  
            is = PropUtil .class.getClassLoader().getResourceAsStream("config.properties");  
            config.load(is);  
        } catch (IOException e) {  
              
        } finally {
        	/**
        	 * close resources
        	 */
            if (is != null) {  
                try {  
                    is.close();  
                } catch (IOException e) {  
                }  
            }  
        }  
        return config;  
    }  
    
    /**
     * @description getInt
     * @param key
     * @return INTEGER
     */
    public static int getInt(String key) {
    	if(config == null) {
    		config = getPropUtil();
    	}
    	return Integer.parseInt(config.getProperty(key));
    }
    
    /**
     * @description getString
     * @param key
     * @return STRING
     */
    public static String getString(String key) {
    	if(config == null) {
    		config = getPropUtil();
    	}
    	return config.getProperty(key);
    }
    
    /**
     * @description getLong
     * @param key
     * @return LONG
     */
    public static long getLong(String key) {
    	if(config == null) {
    		config = getPropUtil();
    	}
    	return Long.parseLong(config.getProperty(key));
    }
}  
