package com.nikey.web;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.nikey.hbase.HbaseTablePool;
import com.nikey.mapper.AliveCheckMapper;
import com.nikey.thread.WorkQueue;
import com.nikey.util.HostNameUtil;
import com.nikey.util.JobControllUtil;
import com.nikey.util.PropUtil;
import com.nikey.util.SendEmail;

@Controller
@RequestMapping(value = "/AliveCheckController")
public class AliveCheckController {
	
	@Autowired
	private AliveCheckMapper aliveCheckMapper;
	
	private HTableInterface htable = null;
	
	private String hostName = null;
	
	private void updateHostName() {
		// get the hostname
		InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		if (addr != null) {
			this.hostName = addr.getHostName().toString();
		}
	}
	
	@RequestMapping(value="/mysqlAndHbaseAliveCheck")
	public void mysqlAndHbaseAliveCheck (@RequestParam("password") String password, HttpServletResponse response)
	{
		int status = 200;
		String errorMsg = "";
		if(password != null && password.equals(PropUtil.getString("password"))) {
			// mysql check
			Callable<String> task = new Callable<String>() {
				@Override
				public String call() throws Exception {
					aliveCheckMapper.getNow();
					return "done";
				}
			};
			if(! JobControllUtil.submitJob(task, "mysql connection failure", getClass().getSimpleName())) {
				status = 404; // MySQL Failed
				errorMsg += "MySQL Failed ......\n";
			}
			
			// hbase check
			if(htable == null) {
				try {
					htable = HbaseTablePool.instance().getHtable(PropUtil.getString("hbase_monitordata_name"));
				} catch (Exception e) {
					status = 404; // Hbase Failed
					errorMsg += "Hbase Failed ......\n";
					WorkQueue.instance().setStopWorking(true);
				}
			}
			if(htable != null) {
				task = new Callable<String>() {
					@Override
					public String call() throws Exception {
						// a test rowkey -- whatever
						htable.get(new Get(new byte[1]));
						return "done";
					}
				};
				if(! JobControllUtil.submitJob(task, "Hbase alive test failed!", getClass().getSimpleName())) {
					status = 404; // Hbase Failed
					errorMsg += "Hbase Failed ......\n";
				}
			}
		} else {
			status = 404; // Unauthorized
        	errorMsg += "Authorized Failed ......\n";
		}
		
		// set response status
		response.setStatus(status);
		if(! "".equals(errorMsg)) {
			if (this.hostName == null)
				updateHostName();
			SendEmail.sendMail(this.hostName + " : HbaseWrite AliveCheckController Alert", errorMsg);
		}
	}

}
