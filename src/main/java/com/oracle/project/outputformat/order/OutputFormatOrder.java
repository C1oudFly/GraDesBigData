package com.oracle.project.outputformat.order;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.project.connect.JdbcManager;
import com.oracle.project.dimention.order.OrderDimention;
import com.oracle.project.sql.imp.Order;

public class OutputFormatOrder extends OutputFormat<OrderDimention, Text> {

	@Override
	public RecordWriter<OrderDimention, Text> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<OrderDimention, Text>{

		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, MapValue> hashMap = new LinkedHashMap<>();
		private MapValue mapValue;
		private HashMap<String, String> statisticsMap = new LinkedHashMap<>();
		private String newkeys;
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}
		
		String oid;
		@Override
		public void write(OrderDimention key, Text value) throws IOException, InterruptedException {
			newkeys = key.getDate() + "\t" + key.getOid();
			oid = key.getOid();
			if(key.getSign().equals("success")){
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(0, 0, oid, 0, 0);
					mapValue.setSuccessCount(Integer.parseInt(value.toString()));
				}else {
					mapValue = hashMap.get(key.getDate());
					mapValue.setSuccessCount(mapValue.getSuccessCount() + Integer.parseInt(value.toString()));
				}
				hashMap.put(key.getDate(), mapValue);	
			}else if(key.getSign().equals("refund")){
				
				if(hashMap.get(key.getDate()) == null){
					mapValue = new MapValue(0, 0, oid, 0, 0);
					mapValue.setRefundCount(Integer.parseInt(value.toString()));
				}else {
					mapValue = hashMap.get(key.getDate());
					mapValue.setRefundCount(mapValue.getRefundCount() + Integer.parseInt(value.toString()));
				}
				hashMap.put(key.getDate(), mapValue);
				
			}else if(key.getSign().equals("statistics")){
				int success = hashMap.get(key.getDate()) == null ? 0 : hashMap.get(key.getDate()).getSuccessCount();
				int refund = hashMap.get(key.getDate())== null ? 0 : hashMap.get(key.getDate()).getRefundCount();
				if(statisticsMap.get(newkeys) == null){
					mapValue = new MapValue(success, refund, oid, 0, 0);
					mapValue.setOrderNumber(1);
					mapValue.setOrderCount(Double.parseDouble(value.toString()));
				}else {
					mapValue = hashMap.get(key.getDate());
					mapValue.setOrderNumber(mapValue.getOrderNumber() + 1);
					mapValue.setOrderCount(mapValue.getOrderCount() + Double.parseDouble(value.toString()));
				}
				statisticsMap.put(newkeys, "statistics");
				hashMap.put(key.getDate(), mapValue);
			}
		}
		
		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("order", connection.prepareStatement(configuration.get("order")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s : hashMap.keySet()){
				new Order().setPreparedStatement(psMap.get("order"), s, hashMap.get(s));
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
