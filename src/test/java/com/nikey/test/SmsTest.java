package com.nikey.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dysmsapi.model.v20170525.QuerySendDetailsRequest;
import com.aliyuncs.dysmsapi.model.v20170525.QuerySendDetailsResponse;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsRequest;
import com.aliyuncs.dysmsapi.model.v20170525.SendSmsResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.nikey.util.PropUtil;

public class SmsTest {
	// 产品名称:云通信短信API产品,开发者无需替换
	static final String product = "Dysmsapi";
	// 产品域名,开发者无需替换
	static final String domain = "dysmsapi.aliyuncs.com";
	// 此处需要替换成开发者自己的AK(在阿里云访问控制台寻找)，下面举个例子
	static final String accessKeyId = PropUtil.getString("accessKeyId");
	static final String accessKeySecret = PropUtil.getString("accessKeySecret");

	public static boolean sendSmsApi(String json, String tel, String template) {
		return false;
	}

	public static SendSmsResponse sendSms() throws ClientException {
		// 可自助调整超时时间
		System.setProperty("sun.net.client.defaultConnectTimeout", "10000");
		System.setProperty("sun.net.client.defaultReadTimeout", "10000");
		// 初始化acsClient,暂不支持region化
		IClientProfile profile = DefaultProfile.getProfile("cn-hangzhou", accessKeyId, accessKeySecret);
		DefaultProfile.addEndpoint("cn-hangzhou", "cn-hangzhou", product, domain);
		IAcsClient acsClient = new DefaultAcsClient(profile);
		// 组装请求对象-具体描述见控制台-文档部分内容
		SendSmsRequest request = new SendSmsRequest();
		// 必填:待发送手机号
		request.setPhoneNumbers("");
		// 必填:短信签名-可在短信控制台中找到举个例子
		request.setSignName("道正电气");
		// 必填:短信模板id-可在短信控制台中找到，是id不是名字
		request.setTemplateCode("SMS_129748683");
		// 可选:模板中的变量替换JSON串,如模板内容为"亲爱的${name},您的验证码为${code}"时,此处的值为--必填，与模板相对应
		// request.setTemplateParam("{\"name\":\"Tom\", \"code\":\"123\"}");
		request.setTemplateParam(
				"{\"customer\":\"jtb\",\"pointinfo\":\"123\",\"time\":\"456\",\"event\":\"eventinfo\",\"threhold\":\"threholdinfo\",\"value\":\"valueinfo\"}");
		// 选填-上行短信扩展码(无特殊需求用户请忽略此字段)
		// request.setSmsUpExtendCode("90997");
		// 可选:outId为提供给业务方扩展字段,最终在短信回执消息中将此值带回给调用者
		// request.setOutId("yourOutId");
		// hint 此处可能会抛出异常，注意catch
		SendSmsResponse sendSmsResponse = acsClient.getAcsResponse(request);
		if(sendSmsResponse.getCode() != null && sendSmsResponse.getCode().equals("OK")) {
			System.out.println("发送成功");//请求成功
		} else {
			System.out.println("发送失败，错误码是："+sendSmsResponse.getCode()+"具体错误码信息：");
		}
		
		return sendSmsResponse;
	}

	public static QuerySendDetailsResponse querySendDetails() throws ServerException, ClientException {
		// 可自助调整超时时间
		System.setProperty("sun.net.client.defaultConnectTimeout", "60000");
		System.setProperty("sun.net.client.defaultReadTimeout", "60000");

		// 初始化acsClient,暂不支持region化
		IClientProfile profile = DefaultProfile.getProfile("cn-hangzhou", accessKeyId, accessKeySecret);
		DefaultProfile.addEndpoint("cn-hangzhou", "cn-hangzhou", product, domain);
		IAcsClient acsClient = new DefaultAcsClient(profile);

		// 组装请求对象
		QuerySendDetailsRequest request = new QuerySendDetailsRequest();
		// 必填-号码
		request.setPhoneNumber("");
		// 可选-流水号
		//request.setBizId(bizId);
		// 必填-发送日期 支持30天内记录查询，格式yyyyMMdd
		SimpleDateFormat ft = new SimpleDateFormat("20180416");
		request.setSendDate(ft.format(new Date()));
		// 必填-页大小
		request.setPageSize(10L);
		// 必填-当前页码从1开始计数
		request.setCurrentPage(1L);

		// hint 此处可能会抛出异常，注意catch
		QuerySendDetailsResponse querySendDetailsResponse = acsClient.getAcsResponse(request);
		
		 //获取返回结果
	       if(querySendDetailsResponse.getCode() != null && querySendDetailsResponse.getCode().equals("OK")){
	        //代表请求成功
	      }
		
		return querySendDetailsResponse;
	}

	public static void main(String[] args) throws ClientException, InterruptedException {

		// 发短信
		SendSmsResponse response = sendSms();

		Thread.sleep(3000L);

		
		
		
		// 查明细
		if (response.getCode() != null && response.getCode().equals("OK")) {
			System.out.println("请求成功");
			QuerySendDetailsResponse querySendDetailsResponse = querySendDetails();
			System.out.println("短信明细查询接口返回数据----------------");
			System.out.println("Code=" + querySendDetailsResponse.getCode());
			System.out.println("Message=" + querySendDetailsResponse.getMessage());
			int i = 0;
			for (QuerySendDetailsResponse.SmsSendDetailDTO smsSendDetailDTO : querySendDetailsResponse
					.getSmsSendDetailDTOs()) {
				System.out.println("SmsSendDetailDTO[" + i + "]:");
				System.out.println("Content=" + smsSendDetailDTO.getContent());
				System.out.println("ErrCode=" + smsSendDetailDTO.getErrCode());
				System.out.println("OutId=" + smsSendDetailDTO.getOutId());
				System.out.println("PhoneNum=" + smsSendDetailDTO.getPhoneNum());
				System.out.println("ReceiveDate=" + smsSendDetailDTO.getReceiveDate());
				System.out.println("SendDate=" + smsSendDetailDTO.getSendDate());
				System.out.println("SendStatus=" + smsSendDetailDTO.getSendStatus());
				System.out.println("Template=" + smsSendDetailDTO.getTemplateCode());
			}
			System.out.println("TotalCount=" + querySendDetailsResponse.getTotalCount());
			System.out.println("RequestId=" + querySendDetailsResponse.getRequestId());
		}

	}
}
