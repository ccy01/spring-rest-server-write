package com.nikey.http;

import com.nikey.util.PropUtil;

public class NikeyConsts {
	static final boolean SHOW_SPEC_MODE = false;

	// SERVER PARAMETERS
	static final String SCHEME = PropUtil.getString("SCHEME");
	static final String HOST = PropUtil.getString("HOST");
	static final int PORT = PropUtil.getInt("PORT");
	public static final String BASE_REST_URL = PropUtil.getString("BASE_REST_URL");
	public static final String HISTORY_REST_URL = PropUtil.getString("HISTORY_REST_URL");

	// SERVER ENTITIES
	static final String RESOURCE = "/resource";
	static final String LOG4J_PATH = "log4j.properties";

}
