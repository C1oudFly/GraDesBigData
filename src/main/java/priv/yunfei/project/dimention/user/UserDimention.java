package com.oracle.project.dimention.user;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserDimention implements WritableComparable<UserDimention> {
	private String sign;
	private String date;
	private String u_uid;
	private String u_mid;
	private String u_sd;
	private String p_url;

	public String getP_url() {
		return p_url;
	}

	public void setP_url(String p_url) {
		this.p_url = p_url;
	}

	public String getU_sd() {
		return u_sd;
	}

	public void setU_sd(String u_sd) {
		this.u_sd = u_sd;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}

	public String getU_mid() {
		return u_mid;
	}

	public void setU_mid(String u_mid) {
		this.u_mid = u_mid;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getU_uid() {
		return u_uid;
	}

	public void setU_uid(String u_uid) {
		this.u_uid = u_uid;
	}

	public UserDimention() {
		
	}
	
	public UserDimention(String sign,String date,String u_ud,String u_mid,String u_sd,String p_url) {
		this.sign = sign;
		this.date = date;
		this.u_uid = u_ud;
		this.u_mid = u_mid;
		this.u_sd = u_sd;
		this.p_url = p_url;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sign);
		out.writeUTF(date);
		out.writeUTF(u_uid);
		out.writeUTF(u_mid);
		out.writeUTF(u_sd);
		out.writeUTF(p_url);
	}

	public void readFields(DataInput in) throws IOException {
		this.sign = in.readUTF();
		this.date = in.readUTF();
		this.u_uid = in.readUTF();
		this.u_mid = in.readUTF();
		this.u_sd = in.readUTF();
		this.p_url = in.readUTF();
	}

	public int compareTo(UserDimention o) {
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
		
		tmp = this.u_uid.compareTo(o.u_uid);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_mid.compareTo(o.u_mid);
		if(tmp != 0){
			return tmp;
		}
		
		tmp = this.u_sd.compareTo(o.u_sd);
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
		result = (result*prime) + this.u_uid == null ? 0 : this.u_uid.hashCode();
		result = (result*prime) + this.u_mid == null ? 0 : this.u_mid.hashCode();
		result = (result*prime) + this.u_sd == null ? 0 : this.u_sd.hashCode();
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
		
		UserDimention userDimention = (UserDimention) obj;
		
		if(this.date == null){
			if(userDimention.date != null){
				return false;
			}
		}else if(userDimention.date == null) {
			if(this.date != null){
				return false;
			}
		}else if(!this.date.equals(userDimention.date)) {
			return false;
		}
		
		if(this.sign == null){
			if(userDimention.sign != null){
				return false;
			}
		}else if(userDimention.sign == null) {
			if(this.sign != null){
				return false;
			}
		}else if(!this.sign.equals(userDimention.sign)) {
			return false;
		}
		
		if(this.u_uid == null){
			if(userDimention.u_uid != null){
				return false;
			}
		}else if(userDimention.u_uid == null) {
			if(this.u_uid != null){
				return false;
			}
		}else if(!this.u_uid.equals(userDimention.u_uid)) {
			return false;
		}
		
		if(this.u_mid == null){
			if(userDimention.u_mid != null){
				return false;
			}
		}else if(userDimention.u_mid == null) {
			if(this.u_mid != null){
				return false;
			}
		}else if(!this.u_mid.equals(userDimention.u_mid)) {
			return false;
		}
		
		if(this.u_sd == null){
			if(userDimention.u_sd != null){
				return false;
			}
		}else if(userDimention.u_sd == null) {
			if(this.u_sd != null){
				return false;
			}
		}else if(!this.u_sd.equals(userDimention.u_sd)) {
			return false;
		}
		
		if(this.p_url == null){
			if(userDimention.p_url != null){
				return false;
			}
		}else if(userDimention.p_url == null) {
			if(this.p_url != null){
				return false;
			}
		}else if(!this.p_url.equals(userDimention.p_url)) {
			return false;
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.date + "\t" + this.sign + "\t" + this.u_uid + "\t" + this.u_mid + "\t" + this.u_sd + "\t" + this.p_url;
	}
	
}
