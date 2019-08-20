package com.nikey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * web server 入口
 * 
 * @author jtb
 *
 */
@SpringBootApplication
@MapperScan(value="com.nikey.mapper")
public class App extends SpringBootServletInitializer {
	
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(App.class);
	}
	
	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(App.class, args);
		
		String[] profiles = context.getEnvironment().getActiveProfiles();
		System.out.println("Active Profiles运行在:  "+ Arrays.toString(profiles) + "模式下，空则代表默认");
	}
	
	
	@Bean  
    public MappingJackson2HttpMessageConverter getMappingJackson2HttpMessageConverter() {  
        MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter = new MappingJackson2HttpMessageConverter();  
        //设置日期格式  
        ObjectMapper objectMapper = new ObjectMapper();  
        mappingJackson2HttpMessageConverter.setObjectMapper(objectMapper);  
        //设置中文编码格式  
        List<MediaType> list = new ArrayList<MediaType>();  
        list.add(MediaType.APPLICATION_JSON_UTF8);  
        mappingJackson2HttpMessageConverter.setSupportedMediaTypes(list);  
        return mappingJackson2HttpMessageConverter;  
    }  
	

}
