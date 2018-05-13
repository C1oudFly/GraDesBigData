package com.oracle.project.mapper.realdata;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.dimention.realdata.DataDimention;
import com.oracle.project.utils.ToDate;

public class DataMapper extends Mapper<LongWritable, Text, DataDimention, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DataDimention, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String [] ljdata = value.toString().split("\t");
		String date = null;
		String type = null;
		String region = null;
		String housing = null;
		String area = null;
		String amount = null;
		String adpartment = null;
		DataDimention dataDimention = null;
		
		//================================每日的成交额=========================
		adpartment = ljdata [0];
		date = ToDate.toDateL(adpartment.split(",")[0]);
		amount = ljdata[4];
		dataDimention = new DataDimention("amount", date);
		context.write(dataDimention, new DoubleWritable(Double.parseDouble(amount)));
		
		//=================================户型总数===========================
		type = adpartment.split(",")[1];
		dataDimention = new DataDimention("type", type);
		context.write(dataDimention, new DoubleWritable(1));
		
		//=================================地址===============================
		region = ljdata[1];
		dataDimention = new DataDimention("region", region);
		context.write(dataDimention, new DoubleWritable(1));
	
		//=================================房屋用途===============================
		housing = ljdata[2];
		dataDimention = new DataDimention("housing", housing);
		context.write(dataDimention, new DoubleWritable(1));
		
		//=================================房屋面积===============================
		area = ljdata[3];
		dataDimention = new DataDimention("area", area);
		context.write(dataDimention, new DoubleWritable(1));	
	}
}
