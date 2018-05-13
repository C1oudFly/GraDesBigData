package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.outputformat.order.MapValue;

public class Order {
	public void setPreparedStatement(PreparedStatement ps, String key, MapValue value) {
		
		try {
			
			ps.setString(1, key);
			ps.setString(2, value.getOidNumber());
			ps.setString(3, Integer.toString(value.getSuccessCount()));
			ps.setString(4, Integer.toString(value.getRefundCount()));
			ps.setString(5, Integer.toString(value.getOrderNumber()));
			ps.setString(6, Double.toString(value.getOrderCount()));
			
			ps.setString(7, value.getOidNumber());
			ps.setString(8, Integer.toString(value.getSuccessCount()));
			ps.setString(9, Integer.toString(value.getRefundCount()));
			ps.setString(10, Integer.toString(value.getOrderNumber()));
			ps.setString(11, Double.toString(value.getOrderCount()));
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
