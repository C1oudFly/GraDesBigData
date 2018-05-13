package com.oracle.project.mapper.order;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.dimention.order.OrderDimention;
import com.oracle.project.utils.ToDate;
import com.oracle.project.utils.UrlPropertyUtils;

public class OrderMapper extends Mapper<LongWritable, Text, OrderDimention, Text> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderDimention, Text>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("\t");
		String date =ToDate.toDate(lines[1]);
		String url = lines[3];
		
		Map<String, String> map = UrlPropertyUtils.toMap(url);
		
		//==================================订单支付成功个数==============================
		String en = map.get(UrlPropertyUtils.KEY_EVENT);
		if(en != null && en.equals("e_cs")){
			OrderDimention orderDimention = new OrderDimention(date, "success", en, "-","-");
			
			context.write(orderDimention, new Text("1"));
		}
		
		//==================================订单退款个数=================================
		en = map.get(UrlPropertyUtils.KEY_EVENT);
		if(en != null && en.equals("e_cr")){
			OrderDimention orderDimention = new OrderDimention(date, "refund", "-", en,"-");
			
			context.write(orderDimention, new Text("1"));
		}
		
		//==================================订单统计=====================================
		String oid = map.get(UrlPropertyUtils.KEY_ORDER);
		String cua = map.get(UrlPropertyUtils.KEY_AMOUNT);
		if(oid != null && cua != null){
			OrderDimention orderDimention = new OrderDimention(date, "statistics", "-", "-", oid);
			
			context.write(orderDimention, new Text(cua));
		}
		
	}
}
