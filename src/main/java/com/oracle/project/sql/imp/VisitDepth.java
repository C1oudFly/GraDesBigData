package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class VisitDepth {
	public void setPreparedStatement(PreparedStatement ps, String key, String value) {
		
		try {
			
			ps.setString(1, key.split("\t")[0]);
			ps.setString(2, key.split("\t")[1]);
			ps.setString(3, value);
			
			ps.setString(4, value);
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
