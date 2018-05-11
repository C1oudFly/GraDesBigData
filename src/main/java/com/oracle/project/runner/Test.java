package com.oracle.project.runner;

import java.net.URLDecoder;

import com.oracle.project.utils.UserAgentUtils;

public class Test {

	public static void main(String[] args) {
		
		String agent="Mozilla%2F5.0%20(compatible%3B%20MSIE%2010.0%3B%20Windows%20NT%206.1%3B%20WOW64%3B%20Trident%2F7.0%3B%20LCTE)";
//		String agentDecoder=URLDecoder.decode(agent);
//		UserAgentInfo userAgentInfo=UserAgentUtils.getUserAgentInfo(agentDecoder);
//		System.out.println(userAgentInfo.getUaFamily());
		
		
		System.out.println(UserAgentUtils.getUserAgentInfo(URLDecoder.decode(agent)).getUaFamily());
		
//		try {
//			System.out.println(URLDecoder.decode("%E4%BA%BA%E6%B0%91%E5%B8%81","UTF-8"));
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
}
