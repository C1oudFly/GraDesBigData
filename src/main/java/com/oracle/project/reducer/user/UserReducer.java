package com.oracle.project.reducer.user;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.dimention.user.UserDimention;

public class UserReducer extends Reducer<UserDimention, LongWritable, UserDimention, LongWritable> {
	
	@Override
	protected void reduce(UserDimention key, Iterable<LongWritable> value,
			Reducer<UserDimention, LongWritable, UserDimention, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		
		if(key.getSign().equals("sessionlength")){
			
			long max = 0;
			long min = 0;
			for(LongWritable l : value){
				if(max == 0){
					max = l.get();
					min = l.get();
				}else {
					if(min > l.get()){
						min = l.get();
					}
					if(max < l.get()){
						max = l.get();
					}
				}
			}
			context.write(key, new LongWritable(max-min));
		}else {
			
			
			for(LongWritable i : value){
				count += i.get();
			}
			context.write(key, new LongWritable(count));
		}

	}

}
