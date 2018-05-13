package com.oracle.project.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.clean.ReaCleanMapper;

public class ReaCleanRunner implements Tool {
	Configuration conf;
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(ReaCleanMapper.class);
		
		job.setMapperClass(ReaCleanMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/realdata"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://yunfei1:9000/rdoutput3"));
		
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new ReaCleanRunner(), args);
		System.out.println(result);
	}

}
