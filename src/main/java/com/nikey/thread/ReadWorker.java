package com.nikey.thread;

import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.nikey.hbase.HTableMapper;

/**
 * @author jayzee
 * @date 27 Sep, 2014
 *	worker of read
 */
public class ReadWorker implements Callable<ResultScanner>{
	
	private final HTableMapper mapper;
	private final Scan scan;
	
	public ReadWorker(HTableMapper mapper, Scan scan) {
		/**
		 * own a htable mapper to work
		 */
		this.mapper = mapper;
		/**
		 * use the scan to work
		 */
		this.scan = scan;
	}

	@Override
	public ResultScanner call() throws Exception {
		return mapper.getResultScannerWithParameterMap(scan);
	}

}
