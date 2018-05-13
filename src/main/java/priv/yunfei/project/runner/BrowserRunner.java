package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.dimention.browser.BrowserDimention;
import com.oracle.project.mapper.browser.BrowserMapper;
import com.oracle.project.outputformat.browser.OutputFormatBrowserUpdate;
import com.oracle.project.reducer.browser.BrowserReducer;

public class BrowserRunner implements Tool {

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
		job.setJarByClass(BrowserRunner.class);
		
		job.setMapperClass(BrowserMapper.class);
		job.setMapOutputKeyClass(BrowserDimention.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(BrowserReducer.class);
		job.setOutputKeyClass(BrowserDimention.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/output/part-r-00000"));
		job.setOutputFormatClass(OutputFormatBrowserUpdate.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new BrowserRunner(), args);
		System.out.println(result);
	}
}
