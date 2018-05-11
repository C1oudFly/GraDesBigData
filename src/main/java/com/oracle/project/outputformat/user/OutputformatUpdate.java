package com.oracle.project.outputformat.user;

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
import com.oracle.project.dimention.user.UserDimention;
import com.oracle.project.sql.imp.BigTable;
import com.oracle.project.sql.imp.VisitDepth;

public class OutputformatUpdate extends OutputFormat<UserDimention, LongWritable> {

	@Override
	public RecordWriter<UserDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<UserDimention, LongWritable>{
		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private MapValue mapValue;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, MapValue> hashMap = new LinkedHashMap();
		private HashMap<String, Integer> visitdepthMap = new LinkedHashMap();
		private String newkey;
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}

		int count = 0;
		
		@Override
		public void write(UserDimention key, LongWritable value) throws IOException, InterruptedException {
			
			if(key.getSign().equals("adduser")){
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setAdduserCount(Integer.parseInt(value.toString()));
				}else{
					
					mapValue = hashMap.get(key.getDate());
					mapValue.setAdduserCount(mapValue.getAdduserCount() + Integer.parseInt(value.toString()));
				}
				
				hashMap.put(key.getDate(), mapValue);
				
			}else if(key.getSign().equals("visituser")){
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setVisitCount(1);
				}else{
					mapValue = hashMap.get(key.getDate());
					mapValue.setVisitCount(mapValue.getVisitCount() + 1);
				}
				hashMap.put(key.getDate(), mapValue);
			}else if (key.getSign().equals("addmember")) {
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setAddmemberCount(1);
				}else{
					mapValue = hashMap.get(key.getDate());
					mapValue.setAddmemberCount(mapValue.getAddmemberCount() + 1);
				}
				hashMap.put(key.getDate(), mapValue);
				
			}else if (key.getSign().equals("activemember")) {
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setActiveCount(1);
				}else{
					mapValue = hashMap.get(key.getDate());
					mapValue.setActiveCount(mapValue.getActiveCount() + 1);
				}
				hashMap.put(key.getDate(), mapValue);
				
			}else if (key.getSign().equals("sessionnumber")) {
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setSessionnumberCount(1);
				}else{
					mapValue = hashMap.get(key.getDate());
					mapValue.setSessionnumberCount(mapValue.getSessionnumberCount() + 1);
				}
				hashMap.put(key.getDate(), mapValue);
				
			}else if (key.getSign().equals("sessionlength")) {
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(key.getDate(), 0, 0, 0, 0, 0, 0);
					mapValue.setSessionlengthCount(Double.parseDouble(value.toString())/60000);
				}else{
					mapValue = hashMap.get(key.getDate());
					mapValue.setSessionlengthCount(mapValue.getSessionlengthCount() + Double.parseDouble(value.toString())/60000);
				}
				hashMap.put(key.getDate(), mapValue);
			}else if (key.getSign().equals("visitdepth")) {
				newkey = key.getDate() + "\t" + key.getU_uid();
				if(visitdepthMap.get(newkey) == null){
					visitdepthMap.put(newkey, 1);
				}else {
					visitdepthMap.put(newkey, visitdepthMap.get(newkey) + 1);
				}
			}

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("bigtable", connection.prepareStatement(configuration.get("bigtable")));
				psMap.put("visitdepth", connection.prepareStatement(configuration.get("visitdepth")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			int userCount = 0;
			int memberCount = 0;
			double averagesessionlength = 0;
			for(String date : hashMap.keySet()){
				userCount += hashMap.get(date).getVisitCount();
				hashMap.get(date).setUserCount(userCount);
				
				memberCount += hashMap.get(date).getAddmemberCount();
				hashMap.get(date).setMemberCount(memberCount);
				
				averagesessionlength = hashMap.get(date).getSessionlengthCount() / hashMap.get(date).getSessionnumberCount();
				hashMap.get(date).setAveragesessionlength(averagesessionlength);
			}
			
			for(String date : hashMap.keySet()){
				new BigTable().setPreparedStatement(psMap.get("bigtable"), date, hashMap.get(date));
			}
			
			for(String s : visitdepthMap.keySet()){
				new VisitDepth().setPreparedStatement(psMap.get("visitdepth"), s, Integer.toString(visitdepthMap.get(s)));
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
