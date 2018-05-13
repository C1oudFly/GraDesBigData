package com.oracle.project.reducer.ip;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.dimention.ip.IpDimention;

public class IpReducer extends Reducer<IpDimention, LongWritable, IpDimention, LongWritable> {
	
	@Override
	protected void reduce(IpDimention key, Iterable<LongWritable> value,
			Reducer<IpDimention, LongWritable, IpDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		if(key.getSign().equals("sessionjumpnumber")){
			for(LongWritable l : value){
				count += l.get();
			}
			if(count == 1){
				System.out.println(key.getU_sd());
				context.write(key, new LongWritable(1));
			}
		}else{
			for(LongWritable l : value){
				count += (int) l.get();
			}
			context.write(key, new LongWritable(count));
		}
		
		
		
		
	}
}
