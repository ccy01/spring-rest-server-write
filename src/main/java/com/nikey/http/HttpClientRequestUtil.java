package com.nikey.http;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.codec.net.URLCodec;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nikey.util.PropUtil;

public class HttpClientRequestUtil {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientRequestUtil.class);

    private HttpClientRequestUtil() {
    }

    public static String sendRequest(String method, Map<String, String> paramMap, String BASE_REST_URL) {
        logger.info("HttpClientRequestUtil sendRequest--> on");
        CloseableHttpClient httpClient = null;
        String json = null;
        try {
            httpClient = HttpClientConnectionManager.getInstance().CreateHttpClient();
            HttpClientJsonResponeHandler responeHandler = new HttpClientJsonResponeHandler();
            List<NameValuePair> qparams = translateMapToNameValuePairList(paramMap);
            if (method.equals("GET")) {
                HttpGet req = new HttpGet();
                req.setURI(createURI(BASE_REST_URL, qparams));
                json = httpClient.execute(req, responeHandler);
            } else if (method.equals("POST")) {
                
                //POST请求与GET请求区别！！！不能使用HttpRequestBase，设置请求参数方法不相同
                HttpPost req = new HttpPost();
                req.setHeader("Content-type", "application/x-www-form-urlencoded");
                // 组装uri
                req.setURI(createURI(BASE_REST_URL,null));//qparams传NULL,不构造url请求参数！！！否则请求失败
                // 装填参数
                List<NameValuePair> nvps = new ArrayList<NameValuePair>();
                if (paramMap != null) {
                    for (Entry<String, String> entry : paramMap.entrySet()) {
                        nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                    }
                }
                // 设置参数到请求对象中
                req.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));
                json = httpClient.execute(req, responeHandler);
                
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(PropUtil.getString("ERR023"), e.getMessage());
        } finally {
            if (httpClient != null) {
                try {
                    httpClient.close();
                } catch (Exception ignore) {
                    ignore.printStackTrace();
                }
            }
        }
        logger.info("HttpClientRequestUtil sendRequest--> off");
        return json;
    }

    @SuppressWarnings("deprecation")
    private static URI createURI(String path, List<NameValuePair> qparams) throws Exception {
        URI uri;
        if (qparams != null)
            uri = URIUtils.createURI(NikeyConsts.SCHEME, NikeyConsts.HOST, NikeyConsts.PORT, path,
                    URLEncodedUtils.format(qparams, "UTF-8"), null);
        else
            uri = (new URL(NikeyConsts.SCHEME, NikeyConsts.HOST, NikeyConsts.PORT, path)).toURI();

        logger.info("sending Request. url: " + uri.toString());
        return uri;
    }

    /**
     * Translate the Map object to NameValuePair object list
     * 
     * @param paraMap
     * @return
     */
    private static List<NameValuePair> translateMapToNameValuePairList(Map<String, String> paraMap) {
        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();
        if (paraMap != null) {
            Set<String> keyset = paraMap.keySet();
            Iterator<String> iterator = keyset.iterator();
            while (iterator.hasNext()) {
                String paraName = iterator.next();
                String paraValue = paraMap.get(paraName);
                ParameterNameValuePair pair = new ParameterNameValuePair(paraName, paraValue);
                nameValuePairs.add(pair);
            }
        } else {
            logger.equals("There is no parameters!");
        }
        return nameValuePairs;
    }

}
