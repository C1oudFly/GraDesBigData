package com.oracle.project.clean;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReaCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String ljdata = value.toString();
		String date = null;
		String type = null;
		String dateAndapartment = null;
		String area = null;
		String amount = null;
		String housing = null;
		String address = null;
		Pattern pattern;
		Matcher matcher;
		if(!(ljdata.contains("其他公司成交"))){
			//获取成交额
			pattern = Pattern.compile("(平米,([0-9]+\\.?[0-9]*))");
			matcher = pattern.matcher(ljdata);
			if(matcher.find()){
				amount = matcher.group(2);
				//获取日期与户型
				pattern = Pattern.compile("((([1-9][0-9]{3}\\s{1,}[0-9]{2}\\s{1,}[0-9]{2})|([1-9][0-9]{3}\\.[0-9]{2}\\.[0-9]{2})|([1-9][0-9]{3}#{1,}[0-9]{2}#{1,}[0-9]{2})).*(卫|厅))");
				matcher = pattern.matcher(ljdata);
				if(matcher.find()){
					dateAndapartment = matcher.group()+"\t";
					
				}else{
					//获取日期
					pattern = Pattern.compile("(([1-9][0-9]{3}\\.[0-9]{2}\\.[0-9]{2})|([1-9][0-9]{3}\\s{1,}[0-9]{2}\\s{1,}[0-9]{2}))");
					matcher = pattern.matcher(ljdata);
					if(matcher.find()){
						date = matcher.group()+",";
					}
					//获取户型
					pattern = Pattern.compile("([0-9]*室.*?厅)");
					matcher = pattern.matcher(ljdata);
					if(matcher.find()){
						type = matcher.group()+"\t";
					}
					dateAndapartment = date + type;
				}
				//获取建筑面积
				pattern = Pattern.compile("([0-9]*\\.?[0-9]*平米)");
				matcher = pattern.matcher(ljdata);
				if(matcher.find()){
					area = matcher.group()+"\t";
				}
				//获取房屋用途
				pattern = Pattern.compile("(-[0-9].*?##)(.*?)##");
				matcher = pattern.matcher(ljdata);
				if(matcher.find()){
					housing = matcher.group(2)+"\t";
				}else{
					housing = "暂无数据" + "\t";
				}
				//获取房屋地址
				pattern = Pattern.compile("(,.*)?,(.*?)(([0-9])|(--))*室.*平米");
				matcher = pattern.matcher(ljdata);
				if(matcher.find()){
					if(matcher.group().equals("")){
						address = "暂无数据"+"\t";
					}else{
						address = matcher.group(2)+"\t";
					}
				}else{
					address = "暂无数据" + "\t";
				}
				
				ljdata = dateAndapartment + address +housing + area + amount;
				context.write(new Text(ljdata.toString()), NullWritable.get());
			}
		}	
	}
}