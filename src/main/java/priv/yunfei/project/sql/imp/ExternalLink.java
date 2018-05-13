package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ExternalLink {
	public void setPreparedStatement(PreparedStatement ps, String key,String external,String externalJump,String jumpRate) {
		
		try {
			
			ps.setString(1, key.split("\t")[0]);
			ps.setString(2, key.split("\t")[1]);
			ps.setString(3, external);
			ps.setString(4, externalJump);
			ps.setString(5, jumpRate);
			
			ps.setString(6, external);
			ps.setString(7, externalJump);
			ps.setString(8, jumpRate);
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
