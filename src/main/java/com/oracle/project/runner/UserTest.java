package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.dimention.user.UserDimention;
import com.oracle.project.mapper.user.UserMapper;
import com.oracle.project.outputformat.user.OutputformatUpdate;
import com.oracle.project.reducer.user.UserReducer;

public class UserTest implements Tool {
	private Configuration conf;
	private Connection connection;
	public void setConf(Configuration conf) {
		this.conf = conf;
		conf.addResource("jdbc.xml");
		conf.addResource("project_sql.xml");
		conf.addResource("sqlclasspath.xml");
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserTest.class);
		
		job.setMapperClass(UserMapper.class);
		job.setMapOutputKeyClass(UserDimention.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(UserReducer.class);
		job.setOutputKeyClass(UserDimention.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/output/part-r-000001"));
		job.setOutputFormatClass(OutputformatUpdate.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new UserTest(), args);
		System.out.println(result);
	}

}
