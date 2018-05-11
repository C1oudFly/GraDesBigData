package com.oracle.project.reducer.external;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.dimention.external.ExternalDimention;

public class ExternalReducer extends Reducer<ExternalDimention, LongWritable, ExternalDimention, LongWritable> {
	
	@Override
	protected void reduce(ExternalDimention key, Iterable<LongWritable> value,
			Reducer<ExternalDimention, LongWritable, ExternalDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		if(key.getSign().equals("externalJump")){
			for(LongWritable l : value){
				count += l.get();
			}
			
			if(count == 1){
				context.write(key, new LongWritable(1));
			}
		}else {
			for(LongWritable l : value){
				count += l.get();
			}
			
			context.write(key, new LongWritable(1));
		}
		
	}
}
