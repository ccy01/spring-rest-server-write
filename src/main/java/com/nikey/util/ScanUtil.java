package com.nikey.util;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author jayzee
 * @date 27 Sep, 2014
 *	set up the scan by HttpServletRequest & Qualifier name
 */
public class ScanUtil {

	/**
	 * @date 27 Sep, 2014
	 * @param HttpServletRequest
	 * @param Scan to set up
	 * @param table means which kind of table data your want to scan, refers to <config.properties>
	 * @param qualifier means the qualifier your want to scan, like : Ua, Ub, Uc, refers to <qualifier_mapping.properties>
	 * @throws Exception
	 *	WARN:set up the scan, it's only used for the default request key <CompanyId+DeviceId+StartTime+EndTime>
	 */
	public static Scan defaultSetUpScan(HttpServletRequest request, String table, String... qualifier) throws Exception{
		Scan scan1 = new Scan();
		// CompanyId of row key
		short CompanyId = Short.valueOf(request.getParameter(PropUtil.getString("CompanyId"))); // default one company
		
		// DeviceId of row key 
		short DeviceId = Short.valueOf(request.getParameter(PropUtil.getString("DeviceId"))); // default one deviceId
		
		// InsertTime of row key
		Long StartTime, EndTime;
		try {
			StartTime = Long.valueOf(request.getParameter(PropUtil.getString("StartTime"))); // default time set to long
			EndTime = Long.valueOf(request.getParameter(PropUtil.getString("EndTime"))); // default time set to long
		} catch (Exception e) {
			StartTime = DateUtil.parseHHMMSSToDate(request.getParameter(PropUtil.getString("StartTime"))).getTime(); // and time might be string of 'yyyy-MM-dd HH:mm:ss'
			EndTime = DateUtil.parseHHMMSSToDate(request.getParameter(PropUtil.getString("EndTime"))).getTime(); // and time might be string of 'yyyy-MM-dd HH:mm:ss'
		}
		
		// start key & end key
		scan1.setStartRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(StartTime)));
		scan1.setStopRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(EndTime)));
		
		// column family & qualifier
		// for example : column family U, qualifier 1, 2, 3(stands for ua, ub, uc)
		String value = null;
		for(String qua : qualifier) {
			value = QualifierUtil.getString(table + "_" + qua);
			scan1.addColumn(Bytes.toBytes(value.split(",")[0]), new byte[]{Byte.valueOf(value.split(",")[1])});
		}
		return scan1;
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param table
	 * @param qualifierName
	 * @return byte
	 *	get qualifier byte by table name and qualifier name
	 */
	public static byte getQualifierByteByTableNameAndQualifierName(String table, String qualifierName) {
		String value = QualifierUtil.getString(table + "_" + qualifierName);
		return Byte.valueOf(value.split(",")[1]);
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param cell
	 * @return long
	 *	get the cell's time
	 */
	public static long getTimeByCell(Cell cell) {
		byte[] time_bytes = new byte[8];
		// arraycopy(源数组，源数组要复制的起始位置，目的数组，目的数组要复制的起始位置，复制长度)
		// 从rowArray的(偏移位置+4)的位置拷贝8个byte到时间数组
		System.arraycopy(cell.getRowArray(), cell.getRowOffset() + 4, time_bytes, 0,
			      8);
		long time = Bytes.toLong(time_bytes);
		return time;
	}
	
	public static short getEventTypeByCell(Cell cell) {
		byte[] eventtype_bytes = new byte[1];
		// arraycopy(源数组，源数组要复制的起始位置，目的数组，目的数组要复制的起始位置，复制长度)
		// 从rowArray的(偏移位置+12)的位置拷贝2个byte到数组
		System.arraycopy(cell.getRowArray(), cell.getRowOffset() + 12, eventtype_bytes, 0,
			      1);
		return eventtype_bytes[0];
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param cell
	 * @return short
	 *	get the cell's DeviceId
	 */
	public static short getDeviceIdByCell(Cell cell) {
		byte[] device_id_bytes = new byte[2];
		// arraycopy(源数组，源数组要复制的起始位置，目的数组，目的数组要复制的起始位置，复制长度)
		// 从rowArray的(偏移位置+2)的位置拷贝2个byte到数组
		System.arraycopy(cell.getRowArray(), cell.getRowOffset() + 2, device_id_bytes, 0,
			      2);
		short deviceId = Bytes.toShort(device_id_bytes);
		return deviceId;
	}
	
	/**
	 * @date 27 Sep, 2014
	 * @param cell
	 * @return short
	 *	get the cell's companyId
	 */
	public static short getCompanyIdByCell(Cell cell) {
		byte[] company_id_bytes = new byte[2];
		// arraycopy(源数组，源数组要复制的起始位置，目的数组，目的数组要复制的起始位置，复制长度)
		// 从rowArray的(偏移位置)的位置拷贝2个byte到数组
		System.arraycopy(cell.getRowArray(), cell.getRowOffset(), company_id_bytes, 0,
			      2);
		short companyId = Bytes.toShort(company_id_bytes);
		return companyId;
	}
	
	/**
	 * @date 28 Sep, 2014
	 * @param request
	 * @param qualifierWithTables
	 * @param scanList
	 * @throws Exception
	 *	qualifierWithTables, for example : [["monitordata1", "ua", "ub", "uc"],["monitordata1", "ia", "ib", "ic"]]
	 *	monitordata1 & monitordata2 mean table name
	 *	ua & ub & uc & ia & ib & ic mean qualifier
	 */
	public static void multiDeviceIdSetUpScan(HttpServletRequest request,
			String[][] qualifierWithTables, List<Scan> scanList) throws Exception {
		
		// CompanyId of row key
		short CompanyId = Short.valueOf(request.getParameter(PropUtil.getString("CompanyId"))); // default one company
		
		// InsertTime of row key
		Long StartTime, EndTime;
		try {
			StartTime = Long.valueOf(request.getParameter(PropUtil.getString("StartTime"))); // default time set to long
			EndTime = Long.valueOf(request.getParameter(PropUtil.getString("EndTime"))); // default time set to long
		} catch (Exception e) {
			StartTime = DateUtil.parseHHMMSSToDate(request.getParameter(PropUtil.getString("StartTime"))).getTime(); // and time might be string of 'yyyy-MM-dd HH:mm:ss'
			EndTime = DateUtil.parseHHMMSSToDate(request.getParameter(PropUtil.getString("EndTime"))).getTime(); // and time might be string of 'yyyy-MM-dd HH:mm:ss'
		}
		
		// DeviceId of row key 
		String DeviceIdStringArr[] = ((String) request.getParameter(PropUtil.getString("DeviceId"))).split(","); // default many device id, and it can be one
		for(int i=0; i<DeviceIdStringArr.length; i++) {
			
			short DeviceId = Short.valueOf(DeviceIdStringArr[i]);
			
			// htable & column family & qualifier
			// for example : column family U, qualifier 1, 2, 3(stands for ua, ub, uc)			
			for(String[] qualifierWithTable : qualifierWithTables) {
				Scan scan1 = new Scan();
				
				// start key & end key
				scan1.setStartRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(StartTime)));
				scan1.setStopRow(Bytes.add(Bytes.toBytes(CompanyId), Bytes.toBytes(DeviceId), Bytes.toBytes(EndTime)));
				
				String htable = qualifierWithTable[0];
				for(int j=1; j<qualifierWithTable.length; j++) { 
					String value = QualifierUtil.getString(htable + "_" + qualifierWithTable[j]);
					scan1.addColumn(Bytes.toBytes(value.split(",")[0]), new byte[]{Byte.valueOf(value.split(",")[1])});					
				}
				
				scanList.add(scan1);
			}
		}
	}

	public static void multiDeviceIdSetUpScan(Short CompanyId,
			String deviceIdStr, String[] qualifierWithOneTable,
			List<Scan> scanList, long StartTime, long EndTime, int weight) {
		// DeviceId of row key
		String DeviceIdStringArr[] = deviceIdStr.split(","); // default many
																// device id,
																// and it can be
																// one
		for (int i = 0; i < DeviceIdStringArr.length; i++) {

			short DeviceId = Short.valueOf(DeviceIdStringArr[i]);
			Scan scan1 = new Scan();
			try {
				scan1.setTimeRange(weight, Long.MAX_VALUE);
			} catch (IOException e) {
				e.printStackTrace();
			}
			// start key & end key
			scan1.setStartRow(Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId), Bytes.toBytes(StartTime + 1)));// scan是左闭包，+1
			scan1.setStopRow(Bytes.add(Bytes.toBytes(CompanyId),
					Bytes.toBytes(DeviceId), Bytes.toBytes(EndTime + 1)));

			String htable = qualifierWithOneTable[0];
			for (int j = 1; j < qualifierWithOneTable.length; j++) {
				String value = QualifierUtil.getString(htable + "_"
						+ qualifierWithOneTable[j]);
				scan1.addColumn(Bytes.toBytes(value.split(",")[0]),
						new byte[] { Byte.valueOf(value.split(",")[1]) });
			}
			scanList.add(scan1);
		}
	}

	/**
	 * @param cell
	 * @return byte
	 * rowKey:CompanyId（shortInt）+DeviceId（shortInt）+RecordDate(long)+Tpye(byte)
	 */
	public static byte getDegreeType(Cell cell) {
		return cell.getRowArray()[cell.getRowOffset() + 12];
	}
	
}
