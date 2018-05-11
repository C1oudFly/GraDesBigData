package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.dimention.external.ExternalDimention;
import com.oracle.project.mapper.external.ExternalMapper;
import com.oracle.project.outputformat.external.OutputFormatExternal;
import com.oracle.project.reducer.external.ExternalReducer;

public class ExternalRunner implements Tool {

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
		job.setJarByClass(ExternalRunner.class);
		
		job.setMapperClass(ExternalMapper.class);
		job.setMapOutputKeyClass(ExternalDimention.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(ExternalReducer.class);
		job.setOutputKeyClass(ExternalDimention.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/contextout2/part-r-000002"));
		job.setOutputFormatClass(OutputFormatExternal.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new ExternalRunner(), args);
		System.out.println(result);
	}

}
