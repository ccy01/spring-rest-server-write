package com.nikey.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author jayzee
 * @date 25 Sep, 2014
 *	HTableMapper
 */
public interface HTableMapper {

	/**
	 * @date 25 Sep, 2014
	 * @param value
	 * @return Put
	 *	convert the parameter map to put
	 */
	List<Put> convertParameterMapToPut(Map<String, String[]> request);

	/**
	 * @date 25 Sep, 2014
	 * @param put
	 * @return true : success
	 * @return false : failure after re-try three times
	 *	put the data to hbase
	 */
	boolean put(final List<Put> put);
	
	/**
	 * @date 26 Sep, 2014
	 * @param value
	 * @return ResultScanner or null
	 *	scan the hbase with scan, and then return the result
	 */
	ResultScanner getResultScannerWithParameterMap(Scan scan);
}
