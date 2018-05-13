package com.oracle.project.mapper.ip;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.ip.utils.IPSeekerExt;
import com.oracle.ip.utils.IPSeekerExt.RegionInfo;
import com.oracle.project.dimention.ip.IpDimention;
import com.oracle.project.utils.ToDate;
import com.oracle.project.utils.UrlPropertyUtils;

public class IpMapper extends Mapper<LongWritable, Text, IpDimention, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IpDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("\t");
		//==================================活跃访客地域分析==============================
		String date = ToDate.toDate(lines[1]);
		String ip = lines[0];
		String url = lines[3];
		Map<String, String> map = UrlPropertyUtils.toMap(url);
		
		IPSeekerExt ipSeekerExt = new IPSeekerExt();
		RegionInfo info = ipSeekerExt.analyticIp(ip);
		String region = info.getProvince();
		System.out.println("****************************" + region);
		IpDimention ipDimention = new IpDimention("region",date, region , "-");
		context.write(ipDimention, new LongWritable(1));
		
		//==================================地域会话个数==============================
		
		String u_sd = map.get(UrlPropertyUtils.KEY_SESSION);
		if(u_sd != null){
			IpDimention ipDimention2 = new IpDimention("sessionregion", date, region, u_sd);
			context.write(ipDimention2, new LongWritable(0));
		}
		
		//==================================地域会话跳出个数==============================

			if(u_sd != null){
				IpDimention ipDimention2 = new IpDimention("sessionjumpnumber", date, region, u_sd);
				context.write(ipDimention2, new LongWritable(1));
			}
		
	}
}
