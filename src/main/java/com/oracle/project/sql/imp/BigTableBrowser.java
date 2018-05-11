package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.browser.MapValue;

public class BigTableBrowser {
	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		
		try {
			
			ps.setString(1, key.split("\t")[0]);
			ps.setString(2, key.split("\t")[1]);
			ps.setString(3, Integer.toString(value.getAdduserCount()));
			ps.setString(4, Integer.toString(value.getVisitCount()));
			ps.setString(5, Integer.toString(value.getUserCount()));
			ps.setString(6, Integer.toString(value.getAddmemberCount()));
			ps.setString(7, Integer.toString(value.getActiveCount()));
			ps.setString(8, Integer.toString(value.getMemberCount()));
			ps.setString(9, Integer.toString(value.getSessionnumberCount()));
			ps.setString(10, Integer.toString(value.getSessionlengthdvalue()));
			ps.setString(11, String.format("%.0f", value.getSessionlengthCount()));
			ps.setString(12, String.format("%.2f", value.getAveragesessionlength()));
			ps.setString(13, Integer.toString(value.getPv()));
			
			ps.setString(14, Integer.toString(value.getAdduserCount()));
			ps.setString(15, Integer.toString(value.getVisitCount()));
			ps.setString(16, Integer.toString(value.getUserCount()));
			ps.setString(17, Integer.toString(value.getAddmemberCount()));
			ps.setString(18, Integer.toString(value.getActiveCount()));
			ps.setString(19, Integer.toString(value.getMemberCount()));
			ps.setString(20, Integer.toString(value.getSessionnumberCount()));
			ps.setString(21, Integer.toString(value.getSessionlengthdvalue()));
			ps.setString(22, String.format("%.0f", value.getSessionlengthCount()));
			ps.setString(23, String.format("%.2f", value.getAveragesessionlength()));
			ps.setString(24, Integer.toString(value.getPv()));
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
