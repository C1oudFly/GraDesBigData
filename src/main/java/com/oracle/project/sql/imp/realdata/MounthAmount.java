package com.oracle.project.sql.imp.realdata;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.realdata.MapValue;

public class MounthAmount {
	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		try {
			ps.setString(1, key);
			ps.setString(2, String.format("%.2f", value.getAmount()));
			ps.setString(3, String.format("%.2f", value.getAmount()));
			
			ps.executeUpdate();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
