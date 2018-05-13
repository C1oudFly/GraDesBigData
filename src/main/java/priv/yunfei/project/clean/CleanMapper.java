package com.oracle.project.clean;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.utils.ToDate;

public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split(" ");
		
		String ip = "-";
		String date = "-";
		String method = "-";
		String url = "-";
		String protocal = "-";
		String status = "-";
		String number = "-";
		
		if(lines.length > 0){
			ip = lines[0];
		}
		if(lines.length > 4){
			date = ToDate.toDateH(lines[3]+" "+lines[4]);
		}
		if(lines.length > 5){
			method = lines[5];
		}
		if(lines.length > 6){
			url = lines[6];
		}
		if(lines.length > 7){
			protocal = lines[7];
		}
		if(lines.length > 8){
			status = lines[8];
		}
		if(lines.length > 9){
			number = lines[9];
		}	
		
		if(url.contains("?")){
			context.write(new Text(ip + "\t" + date + "\t" + method + "\t" + url + "\t" + protocal + "\t" + status + "\t" + number), NullWritable.get());
		}
	}
}
