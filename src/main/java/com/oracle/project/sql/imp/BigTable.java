package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.user.MapValue;

public class BigTable {

	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		
		try {
			
			ps.setString(1, key);
			ps.setString(2, Integer.toString(value.getAdduserCount()));
			ps.setString(3, Integer.toString(value.getVisitCount()));
			ps.setString(4, Integer.toString(value.getUserCount()));
			ps.setString(5, Integer.toString(value.getAddmemberCount()));
			ps.setString(6, Integer.toString(value.getActiveCount()));
			ps.setString(7, Integer.toString(value.getMemberCount()));
			ps.setString(8, Integer.toString(value.getSessionnumberCount()));
			ps.setString(9, String.format("%.2f", value.getSessionlengthCount()));
			ps.setString(10, String.format("%.2f", value.getAveragesessionlength()));
			
			ps.setString(11, Integer.toString(value.getAdduserCount()));
			ps.setString(12, Integer.toString(value.getVisitCount()));
			ps.setString(13, Integer.toString(value.getUserCount()));
			ps.setString(14, Integer.toString(value.getAddmemberCount()));
			ps.setString(15, Integer.toString(value.getActiveCount()));
			ps.setString(16, Integer.toString(value.getMemberCount()));
			ps.setString(17, Integer.toString(value.getSessionnumberCount()));
			ps.setString(18, String.format("%.2f", value.getSessionlengthCount()));
			ps.setString(19, String.format("%.2f", value.getAveragesessionlength()));
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
