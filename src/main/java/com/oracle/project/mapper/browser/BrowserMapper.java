package com.oracle.project.mapper.browser;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.dimention.browser.BrowserDimention;
import com.oracle.project.utils.ToDate;
import com.oracle.project.utils.UrlPropertyUtils;
import com.oracle.project.utils.UserAgentUtils;

public class BrowserMapper extends Mapper<LongWritable, Text, BrowserDimention, LongWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, BrowserDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("\t");
		//==================================新增用户浏览器==============================
		String date = ToDate.toDate(lines[1]);
		String url = lines[3];
		
		Map<String, String> map = UrlPropertyUtils.toMap(url);
		String b_iev = map.get(UrlPropertyUtils.KEY_BROWSER);
		
		if(b_iev == null){
			b_iev = "unkonwn";
		}else {
			b_iev = UserAgentUtils.getUserAgentInfo(URLDecoder.decode(b_iev)).getUaFamily();	
		}
		
		//==================================新增用户浏览器==============================
		if(map.get(UrlPropertyUtils.KEY_EVENT).equals("e_l")){
			BrowserDimention browserDimention = new BrowserDimention(date, "adduser", b_iev, "-", "-","-");
			context.write(browserDimention, new LongWritable(1));
		}
		
		//==================================活跃用户浏览器==============================
		
			String u_ud = map.get(UrlPropertyUtils.KEY_USER);
			if(u_ud != null){
				BrowserDimention browserDimention = new BrowserDimention(date, "activeuser", b_iev, u_ud, "-","-");
				context.write(browserDimention, new LongWritable(0));
			}
			
		//==================================新增会员浏览器==============================
			
			String p_url = map.get(UrlPropertyUtils.KEY_NOW_PAGE);
			String u_mid = map.get(UrlPropertyUtils.KEY_MEMBER);
			
			if(p_url != null && p_url.endsWith("demo4.jsp") && u_mid != null){
				BrowserDimention browserDimention = new BrowserDimention(date, "addmember", b_iev,"-", u_mid,"-");
				
				context.write(browserDimention, new LongWritable(0));
			}
		//==================================活跃会员浏览器===============================
			
			if(u_mid != null){
				BrowserDimention browserDimention = new BrowserDimention(date, "activemember", b_iev,"-", u_mid,"-");
				context.write(browserDimention, new LongWritable(0));
			}
			
		//===================================会话个数浏览器==============================
			String u_sd = map.get(UrlPropertyUtils.KEY_SESSION);
			if(u_sd != null ){
				BrowserDimention browserDimention = new BrowserDimention(date, "sessionnumber", b_iev, "-", "-",u_sd);
				context.write(browserDimention, new LongWritable(0));
			}
			
		//==================================会话长度浏览器===============================
			String c_time = map.get(UrlPropertyUtils.KEY_TIME);
			
			if(u_sd != null && c_time != null){
				BrowserDimention browserDimention = new BrowserDimention(date, "sessionlength", b_iev, "-","-",u_sd);
				context.write(browserDimention, new LongWritable(Long.parseLong(c_time)));
			}
			
		//====================================浏览器PV分析===============================
			BrowserDimention browserDimention = new BrowserDimention(date, "browserPV",  b_iev, "-", "-", "-");
			context.write(browserDimention, new LongWritable(1));
	}
}
