package com.oracle.project.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import com.oracle.project.sql.BaseUser;

public class Pshandler {
	
	public static void psHandler(HashMap<String, PreparedStatement> psMap,HashMap<String, Integer> hashMap,String slsql,String insql,String classpath){
		int total = 0;
		try {
			
			
			PreparedStatement ps =  psMap.get(slsql);
			ResultSet rs = ps.executeQuery();
			
			
			while(rs.next()){
				
				total = Integer.parseInt(rs.getString(2));
				System.out.println(total + "==================================================");
			}
			
			for(String date : hashMap.keySet()){
				
				if(total == 0){
					total += hashMap.get(date);
				}
				else {
					total += hashMap.get(date);
				}

				BaseUser bu = (BaseUser) Class.forName(classpath).newInstance();
				bu.setPreparedStatement(psMap.get(insql), date, Integer.toString(total));
			}
			
			total = 0;
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
