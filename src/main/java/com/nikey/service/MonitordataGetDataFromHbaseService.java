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
 * @author jayzee
 * @date 26 Sep, 2014
 *	Monitordata Get Data From Hbase Service Class 
 */
public class MonitordataGetDataFromHbaseService implements GetDataFromHbaseService {

	@Override
	public Map<String, Object> getDataFromHbase(HttpServletRequest request) {
		/**
		 * WARN: default one deviceId, one htable
		 */
//		return singleJob(request);
		/**
		 * WARN: default many deviceId, multi htables
		 */
		return multiJob(request);
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param request
	 * @return
	 *	WARN: singleJob method means that u only need to scan one htable
	 */
	public Map<String, Object> singleJob(HttpServletRequest request) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		
		/**
		 * 1. WARN:locate the HTableMapper in need, <htable_mapper_xxx> should hv config in config.properties
		 */
		HTableMapper mapper = HTableMapperFactory.instance().getHTableMapper(ClassNameUtil.constructClassNameForMapper(PropUtil.getString("htable_mapper_monitordata")));
		/**
		 * 2. construct the scan
		 */
		Scan scan;
		try {
			scan = ScanUtil.defaultSetUpScan(request, PropUtil.getString("htable_mapper_monitordata"), new String[]{"Ua", "Ub", "Uc"});
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
		ResultScanner scanner = ThreadPoolManagerRead.instance().submit(new ReadWorker(mapper, scan), timeout_second);
		/**
		 * 4. get the result and put it into the map
		 * 	if you hv some message in the HttpServletRequest, you should get it out and put it into modelMap
		 */
		if(scanner == null) {
			resultMap.put("state", 503); // 503=service unavailable
		} else {
			// construct map like :
			// key:Ua, value:[[time,value],[time,value],[time,value]...]
			// key:Ub, value:[[time,value],[time,value],[time,value]...]
			// key:Uc, value:[[time,value],[time,value],[time,value]...]
			
			// Ua
			List<Object[]> ua = new ArrayList<>();
			// Ub
			List<Object[]> ub = new ArrayList<>();
			// Uc
			List<Object[]> uc = new ArrayList<>();
			
			// qulifier byte
			byte ua_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ua");
			byte ub_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ub");
			byte uc_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Uc");
			
			for (Result result : scanner) {
				for (Cell cell : result.rawCells()) { 
					// 匹配qualifier:考虑值为空的情况
					byte qulifier = CellUtil.cloneQualifier(cell)[0];
					float value = Bytes.toFloat(CellUtil.cloneValue(cell));
					// if you want to get <CompanyId> or <DeviceId>, then use ScanUtil.getDeviceIdByCell(cell) or ScanUtil.getCompanyIdByCell(cell)
					long time = ScanUtil.getTimeByCell(cell);
					
					// add value to qualifier
					if(qulifier == ua_qualifier) {
						ua.add(new Object[]{ time, value });
					} else if(qulifier == ub_qualifier) {
						ub.add(new Object[]{ time, value });
					} else if(qulifier == uc_qualifier) {
						uc.add(new Object[]{ time, value });
					}
				}
			}
			
			resultMap.put("ua", ua);
			resultMap.put("ub", ub);
			resultMap.put("uc", uc);
		}
		return resultMap;
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
					new String[]{PropUtil.getString("htable_mapper_monitordata"), "Ua", "Ub", "Uc"},
					// scan2 : second table & qulifier you want to scan
					new String[]{PropUtil.getString("htable_mapper_monitordata"), "Ia", "Ib", "Ic"},
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
		for(int i=0; i<scanList.size(); i++) {
			int mod = i % 2;
			switch (mod) {
			case 0: // scan1 use scan1's mapper
				workers.add(new ReadWorker(mapper, scanList.get(i)));
				break;
			case 1: // scan2 use scan2's mapper
				workers.add(new ReadWorker(mapper, scanList.get(i)));
				break;
			default:
				break;
			}
		}
		Queue<ResultScanner> scanners = ThreadPoolManagerRead.instance().submitMultiJob(workers, timeout_second);
		/**
		 * 4. get the result and put it into the map
		 * 	if you hv some message in the HttpServletRequest, you should get it out and put it into modelMap
		 */
		if(scanners == null) {
			resultMap.put("state", 503); // 503=service unavailable
		} else {
			// one deviceId construct map like :
			// key:Ua, value:[[time,value],[time,value],[time,value]...]
			// key:Ub, value:[[time,value],[time,value],[time,value]...]
			// key:Uc, value:[[time,value],[time,value],[time,value]...]
			// key:Ia, value:[[time,value],[time,value],[time,value]...]
			// key:Ib, value:[[time,value],[time,value],[time,value]...]
			// key:Ic, value:[[time,value],[time,value],[time,value]...]
			// ......
			// many deviceId construct map like :
			// deviceId:xx, value:{ key:Ua, value:[[time,value],[time,value],[time,value]...] }
			
			// DeviceId
			Map<String, Object> idMap = new HashMap<String, Object>();
			// Ua
			List<Object[]> uaList = new ArrayList<>();
			// Ub
			List<Object[]> ubList = new ArrayList<>();
			// Uc
			List<Object[]> ucList = new ArrayList<>();
			// Ia
			List<Object[]> iaList = new ArrayList<>();
			// Ib
			List<Object[]> ibList = new ArrayList<>();
			// Ic
			List<Object[]> icList = new ArrayList<>();
			
			// table1's qulifier byte
			byte ua_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ua");
			byte ub_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ub");
			byte uc_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Uc");
			// table2's qulifier byte
			byte ia_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ia");
			byte ib_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ib");
			byte ic_qualifier = ScanUtil.getQualifierByteByTableNameAndQualifierName(PropUtil.getString("htable_mapper_monitordata"), "Ic");
			
			ResultScanner scanner;
			// ResultScanner结果组织方式:
			// deviceId[i] : {scan1, scan2}
			// ...
			// 折半 : 每个deviceId发起了两个htable的查询,即发起了两个Scan
			// 发起一个Scan一定会返回一个ResultScanner(ResultScanner的结果可能为空)
			int deviceIdNum = scanners.size()/2;
			for(int i=0; i<deviceIdNum; i++) {
				short deviceId = -1;
				// result of scan1
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
						if(qulifier == ua_qualifier) {
							uaList.add(new Object[]{ time, value });
						} else if(qulifier == ub_qualifier) {
							ubList.add(new Object[]{ time, value });
						} else if(qulifier == uc_qualifier) {
							ucList.add(new Object[]{ time, value });
						}
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
						if(qulifier == ia_qualifier) {
							iaList.add(new Object[]{ time, value });
						} else if(qulifier == ib_qualifier) {
							ibList.add(new Object[]{ time, value });
						} else if(qulifier == ic_qualifier) {
							icList.add(new Object[]{ time, value });
						}
					}
				}
				if(-1 != deviceId) {
					if(deviceIdNum > 1) {
						// 请求多个deviceId的情况
						// 将数据深拷贝到resultMap中
						idMap.put("ua", new ArrayList<Object[]>(uaList));
						idMap.put("ub", new ArrayList<Object[]>(ubList));
						idMap.put("uc", new ArrayList<Object[]>(ucList));
						idMap.put("ia", new ArrayList<Object[]>(iaList));
						idMap.put("ib", new ArrayList<Object[]>(ibList));
						idMap.put("ic", new ArrayList<Object[]>(icList));
						resultMap.put(String.valueOf(deviceId), new HashMap<String, Object>(idMap));
						// 清空容器留待下一个deviciId使用
						uaList.clear();
						ubList.clear();
						ucList.clear();
						iaList.clear();
						ibList.clear();
						icList.clear();
						idMap.clear();
					} else {
						// 请求一个deviceId的情况
						resultMap.put("ua", new ArrayList<Object[]>(uaList));
						resultMap.put("ub", new ArrayList<Object[]>(ubList));
						resultMap.put("uc", new ArrayList<Object[]>(ucList));
						resultMap.put("ia", new ArrayList<Object[]>(iaList));
						resultMap.put("ib", new ArrayList<Object[]>(ibList));
						resultMap.put("ic", new ArrayList<Object[]>(icList));
					}
				}
			}
		}
		return resultMap;
	}

}
