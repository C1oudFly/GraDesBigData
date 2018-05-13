package com.oracle.project.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class ToDate {
	public static String toDateH(String date){
		SimpleDateFormat in = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss ZZZZZ]", Locale.US);
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd:HH");
		
		try {
			Date date1 = in.parse(date);
			date = out.format(date1);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}
	
	public static String toDate(String date){
		SimpleDateFormat in = new SimpleDateFormat("yyyy-MM-dd:HH");
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Date date1 = in.parse(date);
			date = out.format(date1);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return date;
	}
	
	public static String toDateL(String date){
		if(date.contains("##")){
			date = date.replaceAll("##", "-");
		}else if(date.contains(".")){
			date = date.replaceAll("\\.", "-");
		}else if(date.contains(" ")){
			date = date.replaceAll(" ", "-");
		}
		
		return date;
	}
}
