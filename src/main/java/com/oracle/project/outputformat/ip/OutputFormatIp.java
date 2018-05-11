package com.oracle.project.outputformat.ip;

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
import com.oracle.project.dimention.ip.IpDimention;
import com.oracle.project.sql.imp.Region;

public class OutputFormatIp extends OutputFormat<IpDimention, LongWritable> {

	@Override
	public RecordWriter<IpDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}

	class MysqlRecordWriter extends RecordWriter<IpDimention, LongWritable>{

		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, MapValue> hashMap = new LinkedHashMap();
		private MapValue mapValue;
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}

		int count = 0;
		String newkey;
		@Override
		public void write(IpDimention key, LongWritable value) throws IOException, InterruptedException {
			newkey = key.getDate() + "\t" + key.getIp();
			if(key.getSign().equals("region")){
				System.out.println(key.getIp() + "---------------------------------");
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey, 0, 0, 0, 0);
					mapValue.setRegion((int)value.get());
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setRegion(mapValue.getRegion() + (int)value.get());
				}
				hashMap.put(newkey, mapValue);	
			}else if(key.getSign().equals("sessionregion")){
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey, 0, 0, 0, 0);
					mapValue.setSessionregion(1);
				}else{
					mapValue = hashMap.get(newkey);
					mapValue.setSessionregion(mapValue.getSessionregion() + 1);
				}
				hashMap.put(newkey, mapValue);	
			}else if(key.getSign().equals("sessionjumpnumber")){
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey, 0, 0, 0, 0);
					mapValue.setSessionjumpnumber((int)value.get());
				}else{
					mapValue = hashMap.get(newkey);
					mapValue.setSessionjumpnumber(mapValue.getSessionjumpnumber() + (int)value.get());
				}
				hashMap.put(newkey, mapValue);
			}
				
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("region", connection.prepareStatement(configuration.get("region")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s: hashMap.keySet()){
				if(hashMap.get(s).getSessionregion() == 0){
					hashMap.get(s).setJumprate(0.00);
				}else{
					hashMap.get(s).setJumprate((double)hashMap.get(s).getSessionjumpnumber() / (double)hashMap.get(s).getSessionregion());
				}
			
				new Region().setPreparedStatement(psMap.get("region"), s, hashMap.get(s));
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
