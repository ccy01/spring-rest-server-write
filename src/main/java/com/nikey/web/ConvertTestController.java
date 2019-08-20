package com.nikey.web;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/ConvertTestController")
public class ConvertTestController {

	@RequestMapping("/convertTest")
	@ResponseBody
	public Map<String, Object> convertTest() {
		Map<String, Object> map = new HashMap<>();
		map.put("test", new Date());
		
		return map;
	}
	
}
