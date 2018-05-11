package com.oracle.project.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

public class JdbcManager {
	public static Connection getConnection(Configuration conf){
		Connection connection = null;
		try {
				
			Class.forName(conf.get("driver"));
			connection = DriverManager.getConnection
					(conf.get("url"),conf.get("username"),conf.get("userpwd"));
			
			//connection.setAutoCommit(false);
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}
}