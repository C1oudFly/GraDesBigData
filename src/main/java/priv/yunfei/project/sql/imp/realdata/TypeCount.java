package com.oracle.project.sql.imp.realdata;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.realdata.MapValue;

public class TypeCount {
	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		try {
			ps.setString(1, key);
			ps.setString(2, String.valueOf(value.getCount()));
			ps.setString(3, String.valueOf(value.getCount()));
			
			ps.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
