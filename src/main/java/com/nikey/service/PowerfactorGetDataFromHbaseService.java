package com.nikey.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.nikey.hbase.HTableMapper;
import com.nikey.hbase.HTableMapperFactory;
import com.nikey.thread.ReadWorker;
import com.nikey.thread.ThreadPoolManagerRead;
import com.nikey.util.ClassNameUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.ScanUtil;

/**
 * @author osun
 *	Powerfactor service
 */
public class PowerfactorGetDataFromHbaseService implements GetDataFromHbaseService {

	@Override
	public Map<String, Object> getDataFromHbase(HttpServletRequest request) {
		/**
		 * WARN: default many deviceId, multi htables
		 */
		return multiJob(request);
	}
	
	/**
	 * @date 28 Sep, 2014
	 * @param request
	 * @return Map<String, Object>
	 *	default many deviceId, multi htables
	 *	submit by multi-jobs, the result is all successful or all failure
	 */
	public Map<String, Object> multiJob(HttpServletRequest request) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		
		/**
		 * 1. locate the HTableMapper in need
		 */
		HTableMapper month_mapper = HTableMapperFactory.instance().getHTableMapper(ClassNameUtil.constructClassNameForMapper(PropUtil.getString("htable_mapper_monitordata_month")));
		HTableMapper mapper = HTableMapperFactory.instance().getHTableMapper(ClassNameUtil.constructClassNameForMapper(PropUtil.getString("htable_mapper_monitordata")));
		// locate the next HTableMapper in need here...

		/**
		 * 2. construct the scan
		 */
		List<Scan> scanList = new ArrayList<Scan>(); 
		try {			
			/**
			 * WARN:
			 * scan 1 should be the first htable u want to scan,
			 * scan 2 should be the second htable u want to scan,
			 * but here we use the same htable <monitordata>, it's just for example,
			 * in real usage, u must use different htable
			 */
			ScanUtil.multiDeviceIdSetUpScan(request, new String[][]{
					// scan1 : first table & qulifier you want to scan
					new String[]{PropUtil.getString("htable_mapper_monitordata_month"), "AvgPFT", "MaxPFT", "MinPFT"},
					// scan2 : second table & qulifier you want to scan
					new String[]{PropUtil.getString("htable_mapper_monitordata"), "PFT"},
			}, scanList);
			
		} catch (Exception e) {
			resultMap.put("state", 400); // 400=bad request
			return resultMap;
		}
		/**
		 * 3. submit the job and wait for the result(will not retry)
		 * 	if u hv the time out value in your request, pls get it out and set to timeout_second below
		 * 	else the program will use the web_default_timeout_second(30s)
		 */
		int timeout_second = PropUtil.getInt("web_default_timeout_second");
		List<ReadWorker> workers = new ArrayList<>();
		workers.add(new ReadWorker(month_mapper, scanList.get(0)));
		workers.add(new ReadWorker(mapper, scanList.get(1)));
		Queue<ResultScanner> scanners = ThreadPoolManagerRead.instance().submitMultiJob(workers, timeout_second);
		
		/**
		 * 4. get the result and put it into the map
		 * 	if you hv some message in the HttpServletRequest, you should get it out and put it into modelMap
		 */
		if(scanners == null) {
			resultMap.put("state", 503); // 503=service unavailable
		} else {
			// one deviceId construct map like :
			// key:MaxPFT, key:MinPFT, key:AvgPFT, key:PFT
	
			// PFT
			List<Object[]> PFT = new ArrayList<>();
			float MinPFT = 0, MaxPFT = 0, AvgPFT = 0;
			
			// table1's qulifier byte
			byte MinPFT_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata_month"), "MinPFT");
			byte MaxPFT_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata_month"), "MaxPFT");
			byte AvgPFT_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata_month"), "AvgPFT");
			// table2's qulifier byte
			byte PFT_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "PFT");
						
			ResultScanner scanner;
			// ResultScanner结果组织方式:
			// deviceId[i] : {scan1, scan2}
			// ...
			// 折半 : 每个deviceId发起了两个htable的查询,即发起了两个Scan
			// 发起一个Scan一定会返回一个ResultScanner(ResultScanner的结果可能为空)
			short deviceId = -1;
			// result of scan1
			scanner = scanners.poll();
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					float value = Bytes.toFloat(CellUtil.cloneValue(cell));
					if(-1 == deviceId) deviceId = ScanUtil.getDeviceIdByCell(cell);
					
					// add value to qualifier
					if(qulifier == MinPFT_qualifier) {
						MinPFT = value;
					} else if(qulifier == MaxPFT_qualifier) {
						MaxPFT = value;
					} else if(qulifier == AvgPFT_qualifier) {
						AvgPFT = value;
					}
					
					// 问：如果查两个的平均 最大 最小怎么处理？
				}
			}
			// result of scan2
			scanner = scanners.poll();			
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					float value = Bytes.toFloat(CellUtil.cloneValue(cell));
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					long time = ScanUtil.getTimeByCell(cell);
					if(-1 == deviceId) deviceId = ScanUtil.getDeviceIdByCell(cell);
					
					// add value to qualifier
					if(qulifier == PFT_qualifier) {
						PFT.add(new Object[]{ time, value });
					}
				}
			}
			if(-1 != deviceId) {
				// 请求一个deviceId的情况
				resultMap.put("PFT", PFT);
				resultMap.put("MaxPFT", MaxPFT);
				resultMap.put("MinPFT", MinPFT);
				resultMap.put("AvgPFT", AvgPFT);
			}
		}
		return resultMap;
	}

}
