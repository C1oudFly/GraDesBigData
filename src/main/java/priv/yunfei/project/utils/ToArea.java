package com.oracle.project.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ToArea {
	public static String toArea(String area){
		Pattern pattern = Pattern.compile("[0-9]*\\.?[0-9]*");
		Matcher matcher = pattern.matcher(area);
		if(matcher.find()){
			area = matcher.group();
		}else{
			area = "暂无数据";
		}
		
		double a = Double.parseDouble(area);
		int b = (int) a;
		int c = (b / 10) * 10;
		int d = (((b + 10) / 10) * 10 ) + 10;
		area = c + "—" + d + "平米";
		
		return area;
	}
	
	public static void main(String args[]) {
	System.out.println(toArea("14平米"));
	}
}
