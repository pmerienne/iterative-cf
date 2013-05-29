package com.github.pmerienne.trident.cf.testing;

import java.util.List;

import org.json.simple.JSONValue;

public class DRPCUtils {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static List<List<Object>> extractValues(String drpcResult) {
		List<List<Object>> values = (List) JSONValue.parse(drpcResult);
		return values;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Object extractSingleValue(String drpcResult) {
		List<List<Object>> values = (List) JSONValue.parse(drpcResult);
		return values.get(0).get(0);
	}

}
