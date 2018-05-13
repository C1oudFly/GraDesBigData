package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.dimention.order.OrderDimention;
import com.oracle.project.mapper.order.OrderMapper;
import com.oracle.project.outputformat.order.OutputFormatOrder;
import com.oracle.project.reducer.order.OrderReducer;

public class OrderRunner implements Tool{
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
		job.setJarByClass(OrderRunner.class);
		
		job.setMapperClass(OrderMapper.class);
		job.setMapOutputKeyClass(OrderDimention.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(OrderReducer.class);
		job.setOutputKeyClass(OrderDimention.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/output/part-r-00000"));
		job.setOutputFormatClass(OutputFormatOrder.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new OrderRunner(), args);
		System.out.println(result);
	}

}
