package com.rc.hadoop.hdfs.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonParseUtils {

	public static Object parseJSON(String json, Object jsonObj) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		Object obj = null;
		try {
			obj = mapper.readValue(json, jsonObj.getClass());
		} catch(Exception e) {
			throw e;
		}
		
		return obj;
	}
}
