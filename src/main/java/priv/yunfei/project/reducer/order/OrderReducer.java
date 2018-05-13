package com.oracle.project.reducer.order;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.dimention.order.OrderDimention;

public class OrderReducer extends Reducer<OrderDimention, Text, OrderDimention, Text> {
	
	@Override
	protected void reduce(OrderDimention key, Iterable<Text> value,
			Reducer<OrderDimention, Text, OrderDimention, Text>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0 ;
		if(key.getSign().equals("statistics")){
			for(Text t : value){
				count += Double.parseDouble(t.toString());
			}
			
			context.write(key, new Text(Double.toString(count)) );
		}else {
			for(Text t : value){
				count += Integer.parseInt(t.toString());
			}
			
			context.write(key, new Text(Integer.toString(count)));
		}
		
	}
}
