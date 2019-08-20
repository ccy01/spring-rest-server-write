package com.nikey.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author ouyang
 * @date 2014-12-04
 * @description data format transfer
 *
 */
public class DataTransferUtil{
	
	/**
	 * slfj
	 */
	static Logger logger = LoggerFactory.getLogger(DataTransferUtil.class);
	
	//字符串数组转换为浮点型数组
	 public static float[] transferStrArrTofloatArr(String [] strArr)
	 {
		 float [] floatArr = new float[strArr.length];
		 for(int i=0;i<strArr.length;i++)
		 {
			 floatArr[i] = Float_valueOf(strArr[i], i);
		 }
		 return floatArr;
	 }
	 //字符串数组转换为byte数组
	 public static byte[] transferStrArraToByteArr(String[] strArr)
	 {
		 byte[] byteArr = new byte[strArr.length];
		 for(int i=0;i<strArr.length;i++)
		 {
			 byteArr[i] = Byte.valueOf(strArr[i]);
		 }
		 return byteArr;
	 }
	 
	 public static float Float_valueOf(String param, int i) {
			float value = Float.valueOf(param);
			String valueStr = String.valueOf(value);
			
			String msg = "The " + "xb" + i + " is error in the request-map, it's value is " + value;
			
			// TODO 调试阶段的电参量奇异值判断
			if(valueStr.contains("E")) {
				Integer mi = Integer.valueOf(valueStr.split("E")[1]);
				if(mi > PropUtil.getInt("hbase_number_range") || mi < -PropUtil.getInt("hbase_number_range")) {
					logger.error(PropUtil.getString("ERR006"), msg);
					throw new RuntimeException(msg);
				}
			}
			if(value <0) {
				logger.error(PropUtil.getString("ERR006"), msg);
				throw new RuntimeException(msg);
			}
			return value;		
		}
	 
	 /**
		 * 
		 * @param arr
		 * @return bytebuffer
		 * @description transfer float array to bytebuffer
	 	 */
		public static ByteBuffer transferArrayTobyteArr(float[] arr)
		{
			ByteBuffer byteBuf = ByteBuffer.allocate(arr.length * 4);
			for(int i=0; i<arr.length; i++) {
				byteBuf.put(Bytes.toBytes(arr[i]));
			}	
			return byteBuf;
		}
		
		/**
		 * 
		 * @param arr
		 * @return bytebuffer
		 * @description  tranfer short array to bytebuffer
	 	 */
		public static ByteBuffer transferArrayTobyteArr(short[] arr)
		{
			ByteBuffer byteBuf = ByteBuffer.allocate(arr.length * 2);
			for(int i=0; i<arr.length; i++) {
				byteBuf.put(Bytes.toBytes(arr[i]));
			}	
			return byteBuf;
		}
		/**
		 * 
		 * @param strArr
		 * @return bytebuffer
		 * @description transfer string (in factor short) array to bytebuffer
		 */
		public static ByteBuffer  transferShortStrToByteArr(String[] strArr )
		{
		    ByteBuffer buffer = ByteBuffer.allocate(strArr.length*2);
	        final int DEVICENUMS = strArr.length;
	        for(int i =0 ;i<DEVICENUMS;i++){
	        	buffer.put(Bytes.toBytes(Short.valueOf(strArr[i])));
	        }
	        return buffer;
		}
		
		/**
		 * 
		 * @param strArr
		 * @return bytebuffer
		 * @description transfer string(in factor float) array to bytebuffer
		 */
		public static ByteBuffer  transferFloatStrToByteArr(String[] strArr )
		{
		    ByteBuffer buffer = ByteBuffer.allocate(strArr.length*4);
	        final int DEVICENUMS = strArr.length;
	        for(int i =0 ;i<DEVICENUMS;i++){
	        	buffer.put(Bytes.toBytes(Float.valueOf(strArr[i])));
	        }
	        return buffer;
		}
		/**
		 * 
		 * @param byteArr
		 * @return  List<Short>
		 * @description transfer byte array to list<short>
		 */
		public static List<Short> transferByteArrToShortArr(byte [] byteArray)
		{
			   List<Short>  valueList = new ArrayList<Short>();
			      ByteBuffer buffer = ByteBuffer.allocate(byteArray.length);
			        
					System.out.println("byteArray size is "+ byteArray.length);
					buffer.put(byteArray);
					buffer.position(0);
					int listLength = byteArray.length/2;
					for(int i = 0 ;i<listLength;i++)
					{
						
						short	value = buffer.getShort();
						System.out.println(value);
						valueList.add(value);

					}
					return valueList;
				
		}
		
	/**
	 * 
	 * @param byteArray
	 * @return List<Float>
	 * @description transfer byte array to list<Float>
	 */
		public static List<Float> transferByteArrToFloatArr(byte [] byteArray)
		{
		   List<Float>  valueList = new ArrayList<Float>();
	      ByteBuffer buffer = ByteBuffer.allocate(byteArray.length);
	        
			System.out.println("byteArray size is "+ byteArray.length);
			buffer.put(byteArray);
			buffer.position(0);
			int listLength = byteArray.length/4;
			for(int i = 0 ;i<listLength;i++)
			{
				float	value = buffer.getFloat();
				System.out.println(value);
				valueList.add(value);
			}
			return valueList;
		}
		
}
