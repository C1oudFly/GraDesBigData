package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.ip.MapValue;

public class Region {
	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		
		try {
			
			ps.setString(1, key.split("\t")[0]);
			ps.setString(2, key.split("\t")[1]);
			ps.setString(3, Integer.toString(value.getRegion()));
			ps.setString(4, Integer.toString(value.getSessionregion()));
			ps.setString(5, Integer.toString(value.getSessionjumpnumber()));
			ps.setString(6, String.format("%.2f", value.getJumprate()));
			
			ps.setString(7, Integer.toString(value.getRegion()));
			ps.setString(8, Integer.toString(value.getSessionregion()));
			ps.setString(9, Integer.toString(value.getSessionjumpnumber()));
			ps.setString(10, String.format("%.2f", value.getJumprate()));
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
