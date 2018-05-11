package com.oracle.project.mapper.external;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.dimention.external.ExternalDimention;
import com.oracle.project.utils.ToDate;
import com.oracle.project.utils.UrlPropertyUtils;

public class ExternalMapper extends Mapper<LongWritable, Text, ExternalDimention, LongWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ExternalDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("\t");
		//==================================外链接偏好==============================
		String date = ToDate.toDate(lines[1]);
		String ip = lines[0];
		String url = lines[3];
		Map<String, String> map = UrlPropertyUtils.toMap(url);
		
		String p_url = map.get(UrlPropertyUtils.KEY_NOW_PAGE);
		if(p_url != null && !p_url.contains(p_url)){
			ExternalDimention externalDimention = new ExternalDimention(date, "external", p_url);
			
			context.write(externalDimention, new LongWritable(1));
		}
		
		//==================================外链接偏好==============================
		if(p_url != null && !p_url.contains(p_url)){
			ExternalDimention externalDimention = new ExternalDimention(date, "externalJump", p_url);
			
			context.write(externalDimention, new LongWritable(1));
		}
	}
}
