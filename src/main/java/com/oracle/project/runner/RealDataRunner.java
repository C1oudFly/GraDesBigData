package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.dimention.realdata.DataDimention;
import com.oracle.project.mapper.realdata.DataMapper;
import com.oracle.project.outputformat.realdata.DataOutputformat;
import com.oracle.project.reducer.realdata.DataReducer;

public class RealDataRunner implements Tool {
	
	private Configuration configuration;
	private Connection connection;

	@Override
	public void setConf(Configuration conf) {
		this.configuration = conf;
		configuration.addResource("jdbc.xml");
		configuration.addResource("project_sql.xml");
		configuration.addResource("sqlclasspath.xml");
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(configuration);
		job.setJarByClass(RealDataRunner.class);
		
		job.setMapperClass(DataMapper.class);
		job.setMapOutputKeyClass(DataDimention.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setReducerClass(DataReducer.class);
		job.setOutputKeyClass(DataDimention.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/rdoutput3/part-r-00000"));
		job.setOutputFormatClass(DataOutputformat.class);
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}
	
	public static void main(String [] args) throws Exception{
		int result = ToolRunner.run(new RealDataRunner(), args);
		System.out.println(result);
	}
}
