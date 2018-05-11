package com.oracle.project.outputformat.realdata;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.project.connect.JdbcManager;
import com.oracle.project.dimention.realdata.DataDimention;
import com.oracle.project.sql.imp.realdata.AreaCount;
import com.oracle.project.sql.imp.realdata.DayAmount;
import com.oracle.project.sql.imp.realdata.HousingCount;
import com.oracle.project.sql.imp.realdata.MounthAmount;
import com.oracle.project.sql.imp.realdata.RegionCount;
import com.oracle.project.sql.imp.realdata.TypeCount;
import com.oracle.project.sql.imp.realdata.YearAmount;
import com.oracle.project.utils.ToArea;

public class DataOutputformat extends OutputFormat<DataDimention, DoubleWritable> {

	@Override
	public RecordWriter<DataDimention, DoubleWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(configuration);
		
		return new MysqlRecordWriter(configuration,connection);
	}
	
	public class MysqlRecordWriter extends RecordWriter<DataDimention, DoubleWritable>{
		private Configuration configuration;
		private Connection connection;
		private PreparedStatement ps;
		private HashMap<String, PreparedStatement> psMap = new HashMap<>();
		private HashMap<String, MapValue> dayAmountMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> mounthAmountMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> yearAmountMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> typeMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> regionMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> housingMap = new LinkedHashMap<>();
		private HashMap<String, MapValue> areaMap = new LinkedHashMap<>();
		private MapValue mapValue = null;
		private String [] date = null;
		private String dateMounth = null;
		private String dateYear = null;
		private String area = null;
		
		public MysqlRecordWriter(Configuration configuration, Connection connection) {
			this.configuration = configuration;
			this.connection = connection;
		}

		@Override
		public void write(DataDimention key, DoubleWritable value) throws IOException, InterruptedException {
			if(key.getSign().equals("amount")){

				date = key.getType().split("-");
				dateMounth = date[0] + "-" +date[1];
				dateYear = date[0];
				
				if(dayAmountMap.get(key.getType()) == null){
					mapValue = new MapValue(key.getType());
					mapValue.setAmount(Double.valueOf(value.get()));
				}else {
					mapValue = dayAmountMap.get(key.getType());
					mapValue.setAmount(mapValue.getAmount() + Double.valueOf(value.get()));
				}
				dayAmountMap.put(key.getType(), mapValue);
				
				if(mounthAmountMap.get(dateMounth) == null){
					mapValue = new MapValue(dateMounth);
					mapValue.setAmount(Double.valueOf(value.get()));
				}else {
					mapValue = mounthAmountMap.get(dateMounth);
					mapValue.setAmount(mapValue.getAmount() + Double.valueOf(value.get()));
				}
				mounthAmountMap.put(dateMounth, mapValue);
				
				if(yearAmountMap.get(dateYear) == null){
					mapValue = new MapValue(dateYear);
					mapValue.setAmount(Double.valueOf(value.get()));
				}else {
					mapValue = yearAmountMap.get(dateYear);
					mapValue.setAmount(mapValue.getAmount() + Double.valueOf(value.get()));
				}
				yearAmountMap.put(dateYear, mapValue);
			}else if (key.getSign().equals("type")) {
				if(typeMap.get(key.getType()) == null){
					mapValue = new MapValue(key.getType());
					mapValue.setCount(Integer.valueOf((int) value.get()));
				}else {
					mapValue = typeMap.get(key.getType());
					mapValue.setCount(mapValue.getCount() + Integer.valueOf((int) value.get()));
				}
				typeMap.put(key.getType(), mapValue);	
			}else if (key.getSign().equals("region")) {
				if(regionMap.get(key.getType()) == null){
					mapValue = new MapValue(key.getType());
					mapValue.setCount(Integer.valueOf((int) value.get()));
				}else {
					mapValue = regionMap.get(key.getType());
					mapValue.setCount(mapValue.getCount() + Integer.valueOf((int) value.get()));
				}
				regionMap.put(key.getType(), mapValue);
			}else if (key.getSign().equals("housing")) {
				if(housingMap.get(key.getType()) == null){
					mapValue = new MapValue(key.getType());
					mapValue.setCount(Integer.valueOf((int) value.get()));
				}else {
					mapValue = housingMap.get(key.getType());
					mapValue.setCount(mapValue.getCount() + Integer.valueOf((int) value.get()));
				}
				housingMap.put(key.getType(), mapValue);
			}else if (key.getSign().equals("area")) {
				area = ToArea.toArea(key.getType());
				if(areaMap.get(area) == null){
					mapValue = new MapValue(area);
					mapValue.setCount(Integer.valueOf((int) value.get()));
				}else {
					mapValue = areaMap.get(area);
					mapValue.setCount(mapValue.getCount() + Integer.valueOf((int) value.get()));
				}
				areaMap.put(area, mapValue);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			try {
				psMap.put("year", connection.prepareStatement(configuration.get("year")));
				psMap.put("mounth", connection.prepareStatement(configuration.get("mounth")));
				psMap.put("day", connection.prepareStatement(configuration.get("day")));
				psMap.put("type", connection.prepareStatement(configuration.get("type")));
				psMap.put("rea_region", connection.prepareStatement(configuration.get("rea_region")));
				psMap.put("housing", connection.prepareStatement(configuration.get("housing")));
				psMap.put("area", connection.prepareStatement(configuration.get("area")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s : dayAmountMap.keySet()){
				new DayAmount().setPreparedStatement(psMap.get("day"), s, dayAmountMap.get(s));
			}
			
			for(String s : mounthAmountMap.keySet()){
				new MounthAmount().setPreparedStatement(psMap.get("mounth"), s, mounthAmountMap.get(s));
			}
			
			for(String s : yearAmountMap.keySet()){
				new YearAmount().setPreparedStatement(psMap.get("year"), s, yearAmountMap.get(s));
			}
			
//			for(String s : typeMap.keySet()){
//				new TypeCount().setPreparedStatement(psMap.get("type"), s, typeMap.get(s));
//			}
//			
//			for(String s : regionMap.keySet()){
//				new RegionCount().setPreparedStatement(psMap.get("rea_region"), s, regionMap.get(s));
//			}
//			
//			for(String s : housingMap.keySet()){
//				new HousingCount().setPreparedStatement(psMap.get("housing"), s, housingMap.get(s));
//			}
//			
//			for(String s : areaMap.keySet()){
//				new AreaCount().setPreparedStatement(psMap.get("area"), s, areaMap.get(s));
//			}
			
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
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}
}
