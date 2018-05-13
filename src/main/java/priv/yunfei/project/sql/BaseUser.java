package com.oracle.project.sql;

import java.sql.PreparedStatement;

public interface BaseUser {
	public void setPreparedStatement(PreparedStatement ps,String key,String value);
}
