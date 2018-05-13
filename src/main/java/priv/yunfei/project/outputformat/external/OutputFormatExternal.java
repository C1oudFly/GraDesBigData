package com.oracle.project.outputformat.external;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.project.connect.JdbcManager;
import com.oracle.project.dimention.external.ExternalDimention;
import com.oracle.project.sql.imp.ExternalLink;

public class OutputFormatExternal extends OutputFormat<ExternalDimention, LongWritable> {

	@Override
	public RecordWriter<ExternalDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<ExternalDimention, LongWritable>{

		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, Integer> externalMap = new LinkedHashMap();
		private HashMap<String, Integer> externalJumpMap = new LinkedHashMap();
		private String newkeys;
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}
		
		int count = 0;
		@Override
		public void write(ExternalDimention key, LongWritable value) throws IOException, InterruptedException {
			newkeys = key.getDate() + "\t" + key.getP_url();
			if(key.getSign().equals("external")){
				count = (int) ((externalMap.get(newkeys) == null ? value.get() : externalMap.get(newkeys)) + value.get());
				externalMap.put(newkeys, count);
				System.out.println(externalMap.get(newkeys) + "-----------" + newkeys);
			}else if (key.getSign().equals("externalJump")) {
				count = (int) ((externalJumpMap.get(newkeys) == null ? value.get() : externalJumpMap.get(newkeys)) + value.get());
				externalJumpMap.put(newkeys, count);
			}
			
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				psMap.put("externallink", connection.prepareStatement(configuration.get("externallink")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			int external = 0;
			int externalJump = 0;
			double jumpRate = 0;
			for(String s : externalMap.keySet()){
				external = externalMap.get(s);
				externalJump = externalJumpMap.get(s);
				jumpRate = (double)external / (double)externalJump;
				new ExternalLink().setPreparedStatement(psMap.get("externallink"), s, Integer.toString(external), Integer.toString(externalJump), Double.toString(jumpRate));
			}
			
			for(String s : psMap.keySet()){
				PreparedStatement ps = psMap.get(s);
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

}
