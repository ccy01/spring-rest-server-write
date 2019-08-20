package com.nikey.hbase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.PropUtil;
/**
 * 
 * @author Jayzee
 * @date 2014-09-10
 * @description tablePool
 */
public class HbaseTablePool {
	
	/**
	 * slfj
	 */
	Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * hbase conf
	 */
	private Configuration conf = null;
	private HConnection connection;
	
	/**
	 * connected flag
	 */
	private volatile boolean isConnected = false;
	
	/**
	 * singleton
	 */
	private static final HbaseTablePool pool = new HbaseTablePool();
	private HbaseTablePool() {
		final long time = PropUtil.getInt("hbase_connection_timeout");
		
		conf = HBaseConfiguration.create();
		conf.set(PropUtil.getString("hbase_name"),
				PropUtil.getString("hbase_value"));
		conf.setInt(PropUtil.getString("hbase_handler_name"), PropUtil.getInt("hbase_handler_count"));
		
		try {
			/**
			 * use the volatile field & time to control fist connect timeout
			 */
	        new Thread(new Runnable() {				
				@Override
				public void run() {
					long start = System.currentTimeMillis();
	            	while((System.currentTimeMillis() - start) <= time) {
	            		try {
	            			Thread.sleep(10000);
						} catch (Exception e) {}
	            	}
	            	logErrorAndExit();
				}
			}).start(); 
			
        	connection = HConnectionManager.createConnection(conf);
        	isConnected = true; // connection connect, don't need to exit
		} catch (Exception e) {
			logErrorAndExit();
		}
	}
	public static HbaseTablePool instance() {
		return pool;
	}

	/**
	 * @date 25 Sep, 2014
	 * @param tableName
	 * @return HTableInterface
	 *	get htable interface from hadoop
	 * @throws IOException 
	 */
	public HTableInterface getHtable(String tableName) throws IOException
	{
		return connection.getTable(tableName);
	}
	
	public boolean getIsConnected() {
		return isConnected;
	}
	
	/**
	 * @date 28 Sep, 2014
	 *	log start-up error and exit the program
	 */
	public void logErrorAndExit() {
		if(! isConnected) {
			logger.error("Connect to the hbase failure, the program will be shut down!");
//	    	System.exit(0); // if the first connect to hbase timeout, then the program will be shut down	
		} else {
			logger.warn("The hconnection is connected : {}!", isConnected);
		}
	} 
	
}
