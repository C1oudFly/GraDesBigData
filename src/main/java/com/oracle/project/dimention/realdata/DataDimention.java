package com.oracle.project.dimention.realdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DataDimention implements WritableComparable<DataDimention> {
	private String sign;
	private String type;
	
	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	public DataDimention() {
		
	}
	
	public DataDimention(String sign, String type) {
		this.sign = sign;
		this.type = type;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sign);
		out.writeUTF(type);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.sign = in.readUTF();
		this.type = in.readUTF();
	}

	@Override
	public int compareTo(DataDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.sign.compareTo(o.sign);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.type.compareTo(o.type);
		if(tmp != 0){
			return tmp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int prime = 100;
		int result = 1;
		result = (result*prime) + this.sign == null ? 0 : this.sign.hashCode();
		result = (result*prime) + this.type == null ? 0 : this.type.hashCode();
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		DataDimention dataDimention = (DataDimention) obj;
		
		if (sign == null) {
			if (dataDimention.sign != null)
				return false;
		} else if (!sign.equals(dataDimention.sign))
			return false;
		
		if (type == null) {
			if (dataDimention.type != null)
				return false;
		} else if (!type.equals(dataDimention.type))
			return false;
		
		return true;
	}

	@Override
	public String toString() {
		return this.sign + "\t" + this.type ;
	}	
}
