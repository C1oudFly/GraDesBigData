package com.oracle.project.dimention.external;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ExternalDimention implements WritableComparable<ExternalDimention>{
	
	private String date;
	private String sign;
	private String p_url;
	
	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getP_url() {
		return p_url;
	}

	public void setP_url(String p_url) {
		this.p_url = p_url;
	}

	public ExternalDimention() {
		
	}
	
	public ExternalDimention(String date, String sign, String p_url) {
		super();
		this.date = date;
		this.sign = sign;
		this.p_url = p_url;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(date);
		out.writeUTF(sign);
		out.writeUTF(p_url);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.date = in.readUTF();
		this.sign = in.readUTF();
		this.p_url = in.readUTF();
	}

	@Override
	public int compareTo(ExternalDimention o) {
		if(this == o){
			return 0;
		}
		
		int tmp = this.date.compareTo(o.date);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.sign.compareTo(o.sign);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.p_url.compareTo(o.p_url);
		if(tmp != 0){
			return tmp;
		}
		
		return 0;
	}
	
	@Override
	public int hashCode() {
		int result = 1;
		int prime = 100;
		
		result = (result*prime) + this.date == null ? 0 : this.date.hashCode();
		result = (result*prime) + this.sign == null ? 0 : this.sign.hashCode();
		result = (result*prime) + this.p_url == null ? 0 : this.p_url.hashCode();
		
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj){
			return true;
		}
		
		if(this.getClass() != obj.getClass()){
			return false;
		}
		
		ExternalDimention externalDimention = (ExternalDimention) obj;
		
		if(this.date == null){
			if(externalDimention.date != null){
				return false;
			}
		}else if(externalDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(externalDimention.date)) {
			return false;
		}
		
		if(this.sign == null){
			if(externalDimention.sign != null){
				return false;
			}
		}else if(externalDimention.sign == null) {
			if(this.sign != null){
				return false;
			}
		}else if(!this.sign.equals(externalDimention.sign)) {
			return false;
		}
		
		if(this.p_url == null){
			if(externalDimention.p_url != null){
				return false;
			}
		}else if(externalDimention.p_url == null) {
			if(this.p_url != null){
				return false;
			}
		}else if(!this.p_url.equals(externalDimention.p_url)) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.date + "\t" + this.sign + "\t" + this.p_url;
	}
}
