package com.oracle.project.reducer.realdata;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.dimention.realdata.DataDimention;

public class DataReducer extends Reducer<DataDimention, DoubleWritable, DataDimention, DoubleWritable> {
	
	@Override
	protected void reduce(DataDimention key, Iterable<DoubleWritable> value,
			Reducer<DataDimention, DoubleWritable, DataDimention, DoubleWritable>.Context context) 
			throws IOException, InterruptedException {
		
		Double count = 0.0;
		
		for(DoubleWritable l : value){
			count += l.get();
		}
		
		context.write(key, new DoubleWritable(count));
	}
}
