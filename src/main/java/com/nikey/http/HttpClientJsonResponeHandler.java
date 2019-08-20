package com.nikey.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.protocol.HTTP;

public class HttpClientJsonResponeHandler implements ResponseHandler<String> {

	@Override
	public String handleResponse(HttpResponse response)
			throws ClientProtocolException, IOException {
		StatusLine statusLine = response.getStatusLine();
		HttpEntity entity = response.getEntity();
		if (statusLine.getStatusCode() >= 300) {
			System.out.println(statusLine.getStatusCode());
			throw new HttpResponseException(statusLine.getStatusCode(),
					statusLine.getReasonPhrase());
		}
		if (entity == null) {
			throw new ClientProtocolException("Respone contains no content!");
		}
		StringBuffer result = new StringBuffer();
		@SuppressWarnings("deprecation")
		BufferedReader rd = new BufferedReader(new InputStreamReader(
				entity.getContent(), HTTP.UTF_8));
		String tempLine = rd.readLine();
		while (tempLine != null) {
			result.append(tempLine);
			tempLine = rd.readLine();
		}
		return result.toString();
	}

}
