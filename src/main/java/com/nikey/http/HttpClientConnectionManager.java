package com.nikey.http;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;

import javax.net.ssl.SSLContext;

import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.BrowserCompatHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultHttpResponseParser;
import org.apache.http.impl.conn.DefaultHttpResponseParserFactory;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.apache.http.impl.io.DefaultHttpRequestWriterFactory;
import org.apache.http.io.HttpMessageParser;
import org.apache.http.io.HttpMessageParserFactory;
import org.apache.http.io.HttpMessageWriterFactory;
import org.apache.http.io.SessionInputBuffer;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicLineParser;
import org.apache.http.message.LineParser;
import org.apache.http.util.CharArrayBuffer;

public class HttpClientConnectionManager {
	
	public static HttpClientConnectionManager getInstance(){
		return new HttpClientConnectionManager();
	}
	
	/**
	 * Preapare the configuration of Connecton Manager
	 */
	public CloseableHttpClient CreateHttpClient(){
		CloseableHttpClient httpClient=null;
		// Use custom message parser / writer to customize the way HTTP
	    // messages are parsed from and written out to the data stream.
	    HttpMessageParserFactory<HttpResponse> responseParserFactory = new DefaultHttpResponseParserFactory() {
	        @Override
	        public HttpMessageParser<HttpResponse> create(
	            SessionInputBuffer buffer, MessageConstraints constraints) {
	            LineParser lineParser = new BasicLineParser() {
	                @Override
	                public Header parseHeader(final CharArrayBuffer buffer) {
	                    try {
	                        return super.parseHeader(buffer);
	                    } catch (ParseException ex) {
	                        return new BasicHeader(buffer.toString(), null);
	                    }
	                }

	            };
	            return new DefaultHttpResponseParser(buffer, lineParser, DefaultHttpResponseFactory.INSTANCE, constraints) {
	                @Override
	                protected boolean reject(final CharArrayBuffer line, int count) {
	                    // try to ignore all garbage preceding a status line infinitely
	                    return false;
	                }

	            };
	        }

	    };
	    HttpMessageWriterFactory<HttpRequest> requestWriterFactory = new DefaultHttpRequestWriterFactory();

	    /* Use a custom connection factory to customize the process of
	     * initialization of outgoing HTTP connections. Beside standard connection
	     * configuration parameters HTTP connection factory can define message
	     * parser / writer routines to be employed by individual connections.
	     */
	    HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory = new ManagedHttpClientConnectionFactory(
	            requestWriterFactory, responseParserFactory);

	    /** Client HTTP connection objects when fully initialized can be bound to
	     * an arbitrary network socket. The process of network socket initialization,
	     * its connection to a remote address and binding to a local one is controlled
	     * by a connection socket factory.
	     * SSL context for secure connections can be created either based on
	     *system or application specific properties.
	    */
	    SSLContext sslcontext = SSLContexts.createSystemDefault();
	    // Use custom hostname verifier to customize SSL hostname verification.
	    X509HostnameVerifier hostnameVerifier = new BrowserCompatHostnameVerifier();

	    // Create a registry of custom connection socket factories for supported
	    // protocol schemes.
	    Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
	        .register("http", PlainConnectionSocketFactory.INSTANCE)
	        .register("https", new SSLConnectionSocketFactory(sslcontext, hostnameVerifier))
	        .build();

	    // Use custom DNS resolver to override the system DNS resolution.
	    DnsResolver dnsResolver = new SystemDefaultDnsResolver() {

	        @Override
	        public InetAddress[] resolve(final String host) throws UnknownHostException {
	            if (host.equalsIgnoreCase("localhost")) {
	                return new InetAddress[] { InetAddress.getByAddress(new byte[] {127, 0, 0, 1}) };
	            } else {
	                return super.resolve(host);
	            }
	        }
	    };
	    httpClient=CreateConnectionManager(socketFactoryRegistry, connFactory, dnsResolver);
	    
	    return httpClient;
	}

    /**
     *  Create a connection manager with custom configuration.
     * @param socketFactoryRegistry
     * @param connFactory
     * @param dnsResolver
     */
    private  CloseableHttpClient  CreateConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,HttpConnectionFactory<HttpRoute, ManagedHttpClientConnection> connFactory ,DnsResolver dnsResolver){
	    // Create a connection manager with custom configuration.
	    PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry, connFactory, dnsResolver);
	    
	    // Create socket configuration
	    SocketConfig socketConfig = SocketConfig.custom().setTcpNoDelay(true).build();
	    
	    // Configure the connection manager to use socket configuration either
	    // by default or for a specific host.
	    connManager.setDefaultSocketConfig(socketConfig);
	    
	    //This includes remote host name, port and scheme.
	    connManager.setSocketConfig(new HttpHost(NikeyConsts.HOST,NikeyConsts.PORT), socketConfig);
	
	    // Create message constraints
//	    Builder messageConstraintsBuilder=MessageConstraints.custom();
//	    messageConstraintsBuilder.setMaxHeaderCount(200);
//	    messageConstraintsBuilder.setMaxLineLength(2000);
//	    MessageConstraints messageConstraints= messageConstraintsBuilder.build();
	    MessageConstraints messageConstraints = MessageConstraints.custom()
	    		.setMaxHeaderCount(200)
	    		.setMaxLineLength(2000)
	    		.build();
	    
	    // Create connection configuration
	    ConnectionConfig connectionConfig = ConnectionConfig.custom()
	    		.setMalformedInputAction(CodingErrorAction.IGNORE)
	    		.setUnmappableInputAction(CodingErrorAction.IGNORE)
	    		.setCharset(Consts.UTF_8)
	    		.setMessageConstraints(messageConstraints)
	    		.build();
	    // Configure the connection manager to use connection configuration either
	    // by default or for a specific host.
	    connManager.setDefaultConnectionConfig(connectionConfig);
	    connManager.setConnectionConfig(new HttpHost(NikeyConsts.HOST,NikeyConsts.PORT), ConnectionConfig.DEFAULT);
	
	    // Configure total max or per route limits for persistent connections
	    // that can be kept in the pool or leased by the connection manager.
	    connManager.setMaxTotal(1000);
	    connManager.setDefaultMaxPerRoute(100);
	    connManager.setMaxPerRoute(new HttpRoute(new HttpHost(NikeyConsts.HOST,NikeyConsts.PORT)), 200);
	
	    // Use custom cookie store if necessary.
	    CookieStore cookieStore = new BasicCookieStore();
	    // Use custom credentials provider if necessary.
	    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	    
	    RequestConfig defaultRequestConfig=this.getDefaultRequstConfig();
	
	    // Create an HttpClient with the given custom dependencies and configuration.
	    CloseableHttpClient httpclient = HttpClients.custom()
	        .setConnectionManager(connManager)
	        .setDefaultCookieStore(cookieStore)
	        .setDefaultCredentialsProvider(credentialsProvider)
	        .setDefaultRequestConfig(defaultRequestConfig)
	        .build();
	    
		return httpclient;
    }
    
    public RequestConfig getDefaultRequstConfig(){
	    // Create global request configuration
	    RequestConfig requestConfig = RequestConfig.custom()
	        .setCookieSpec(CookieSpecs.BEST_MATCH)
	        .setExpectContinueEnabled(true)
	        .setStaleConnectionCheckEnabled(true)
			.setSocketTimeout(5000)
			.setConnectionRequestTimeout(5000)
	        .setTargetPreferredAuthSchemes(Arrays.asList(AuthSchemes.NTLM, AuthSchemes.DIGEST))
	        .setProxyPreferredAuthSchemes(Arrays.asList(AuthSchemes.BASIC))
	        .build();
	    return requestConfig;
    }
}
