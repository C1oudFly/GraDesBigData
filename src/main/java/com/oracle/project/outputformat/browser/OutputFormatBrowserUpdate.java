package com.oracle.project.outputformat.browser;

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
import com.oracle.project.dimention.browser.BrowserDimention;
import com.oracle.project.sql.imp.BigTableBrowser;

public class OutputFormatBrowserUpdate extends OutputFormat<BrowserDimention, LongWritable> {

	@Override
	public RecordWriter<BrowserDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	public class MysqlRecordWriter extends RecordWriter<BrowserDimention, LongWritable>{
		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private String newkey;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, Integer> countMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, MapValue> hashMap = new LinkedHashMap<String, MapValue>();
		private MapValue mapValue;
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}
		
		@Override
		public void write(BrowserDimention key, LongWritable value) throws IOException, InterruptedException {
			newkey = key.getDate() + "\t" + key.getB_iev();
			if(key.getSign().equals("adduser")){
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setAdduserCount((int)value.get());
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setAdduserCount(mapValue.getAdduserCount() + (int)value.get());
				}
				hashMap.put(newkey, mapValue);	
			}else if (key.getSign().equals("activeuser")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setVisitCount(1);
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setVisitCount(mapValue.getVisitCount() + 1);
				}
				
				hashMap.put(newkey, mapValue);
			}else if (key.getSign().equals("addmember")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setAddmemberCount(1);
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setAddmemberCount(mapValue.getAddmemberCount() + 1);
				}
				
				hashMap.put(newkey, mapValue);	
			}else if (key.getSign().equals("activemember")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setActiveCount(1);

				}else {
					
					mapValue = hashMap.get(newkey);
					mapValue.setActiveCount(mapValue.getActiveCount() + 1);
				}
		
				hashMap.put(newkey, mapValue);
			}else if (key.getSign().equals("sessionnumber")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setSessionnumberCount(1);
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setSessionnumberCount(mapValue.getSessionnumberCount() + 1);
				}
		
				hashMap.put(newkey, mapValue);
			}else if (key.getSign().equals("sessionlength")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setSessionlengthdvalue((int) value.get());
					mapValue.setSessionlengthCount(1);
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setSessionlengthdvalue(mapValue.getSessionlengthdvalue() + (int) value.get());
					mapValue.setSessionlengthCount(mapValue.getSessionlengthCount() + 1);
				}
		
				hashMap.put(newkey, mapValue);
			}else if (key.getSign().equals("browserPV")) {
				
				if(hashMap.get(newkey) == null){
					mapValue = new MapValue(newkey);
					mapValue.setPv((int) value.get());
				}else {
					mapValue = hashMap.get(newkey);
					mapValue.setPv(mapValue.getPv() + (int)value.get());
				}
		
				hashMap.put(newkey, mapValue);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("bigtablebrowser", connection.prepareStatement(configuration.get("bigtablebrowser")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s : hashMap.keySet()){
				if(hashMap.get(s).getSessionlengthCount() == 0){
					hashMap.get(s).setAveragesessionlength(0);
				}else {
					hashMap.get(s).setAveragesessionlength(hashMap.get(s).getSessionlengthdvalue() / hashMap.get(s).getSessionlengthCount());
				}
			}
			
			String news;
			for(String s : hashMap.keySet()){
				news = s.split("\t")[1];
				countMap.put(news, (countMap.get(news) == null ? 0 : countMap.get(news)) + hashMap.get(s).getVisitCount());
				hashMap.get(s).setUserCount(countMap.get(news));
			}
			
			countMap.clear();
			for(String s : hashMap.keySet()){
				news = s.split("\t")[1];
				countMap.put(news, (countMap.get(news) == null ? 0 : countMap.get(news)) + hashMap.get(s).getAddmemberCount());
				hashMap.get(s).setMemberCount(countMap.get(news));
			}
			
			for(String s : hashMap.keySet()){
				new BigTableBrowser().setPreparedStatement(psMap.get("bigtablebrowser"), s, hashMap.get(s));
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
