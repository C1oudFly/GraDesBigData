package com.oracle.project.utils;

import java.util.HashMap;
import java.util.Map;

public class UrlPropertyUtils {

	public static final String KEY_WEBSITE = "website";
	public static final String KEY_EVENT = "en";
	public static final String KEY_VERSION = "ver";
	public static final String KEY_PLATFORM = "pl";
	public static final String KEY_SDK = "sdk";
	public static final String KEY_RESOLUTION = "b_rst";
	public static final String KEY_BROWSER = "b_iev";
	public static final String KEY_USER = "u_ud";
	public static final String KEY_LANGUAGE = "l";
	public static final String KEY_MEMBER = "u_mid";
	public static final String KEY_SESSION = "u_sd";
	public static final String KEY_TIME = "c_time";
	public static final String KEY_NOW_PAGE = "p_url";
	public static final String KEY_PREV_PAGE = "p_ref";
	public static final String KEY_ORDER = "oid";
	public static final String KEY_AMOUNT = "cua";

	public static Map<String, String> toMap(String origin) {
		Map<String, String> map = new HashMap<String, String>();
		String[] strs = origin.split("\\?");
		map.put(KEY_WEBSITE, strs[0]);
		String[] properties = strs[1].split("\\&");
		for (String item : properties) {
			String[] content = item.split("=");
			map.put(content[0], content[1]);
		}
		return map;
	}
}
