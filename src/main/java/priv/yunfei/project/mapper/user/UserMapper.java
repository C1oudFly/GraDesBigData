package com.oracle.project.mapper.user;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.dimention.user.UserDimention;
import com.oracle.project.utils.ToDate;
import com.oracle.project.utils.UrlPropertyUtils;
import com.oracle.project.utils.UserAgentUtils;

public class UserMapper extends Mapper<LongWritable, Text, UserDimention, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {

		String lines [] = value.toString().split("\t");
		//==================================新增用户==============================
		String date =ToDate.toDate(lines[1]);
		String url = lines[3];
		
		Map<String, String> map = UrlPropertyUtils.toMap(url);
		String b_iev = map.get(UrlPropertyUtils.KEY_BROWSER);
		
		if(b_iev == null){
			b_iev = "unkonwn";
		}else {
			b_iev = UserAgentUtils.getUserAgentInfo(URLDecoder.decode(b_iev)).getUaFamily();	
		}
		
		if(map.get(UrlPropertyUtils.KEY_EVENT).equals("e_l")){
			UserDimention userDimention = new UserDimention("adduser", date, "-", "-","-","-");
			context.write(userDimention, new LongWritable(1));
		}
		
		//==================================活跃用户==============================
		
		String u_ud = map.get(UrlPropertyUtils.KEY_USER);
		
		
		if(u_ud != null){
			UserDimention userDimention = new UserDimention("visituser", date, u_ud, "-","-","-");
			context.write(userDimention, new LongWritable(0));
		}
			
			
		//==================================新增会员===============================
		
		String p_url = map.get(UrlPropertyUtils.KEY_NOW_PAGE);
		String u_mid = map.get(UrlPropertyUtils.KEY_MEMBER);
		
		if(p_url != null && p_url.endsWith("demo4.jsp") && u_mid != null){
			UserDimention userDimention = new UserDimention("addmember", date, "-", u_mid,"-","-");
			
			context.write(userDimention, new LongWritable(0));
		}
		
			
		//==================================活跃会员===============================
		
		if(u_mid != null){
			UserDimention userDimention = new UserDimention("activemember", date, "-", u_mid,"-","-");
			context.write(userDimention, new LongWritable(0));
		}
		
		//===================================会话个数==============================
		String u_sd = map.get(UrlPropertyUtils.KEY_SESSION);
		if(u_sd != null ){
			UserDimention userDimention = new UserDimention("sessionnumber", date, "-", "-", u_sd,"-");
			context.write(userDimention, new LongWritable(0));
		}
		
		
		//==================================会话长度===============================
		
		String c_time = map.get(UrlPropertyUtils.KEY_TIME);
		
		if(u_sd != null && c_time != null){
			UserDimention userDimention = new UserDimention("sessionlength", date, "-", "-", u_sd,"-");
			context.write(userDimention, new LongWritable(Long.parseLong(c_time)));
		}
		
		//==================================用户访问深度===============================
		if(u_ud != null && p_url != null){
			UserDimention userDimention = new UserDimention("visitdepth", date, u_ud, "-", "-", p_url);
			context.write(userDimention, new LongWritable(0));
		}
		
	}
}
