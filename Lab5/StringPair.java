package comp9313.lab5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringPair implements WritableComparable<StringPair> {

	private String first;
	private String second;

	// 构造函数-1
	public StringPair() {
	}

	// 构造函数-2
	public StringPair(String first, String second) {
		set(first, second);
	}

	// 赋值函数
	public void set(String left, String right) {
		first = left;
		second = right;
	}

	// 取值函数
	public String getFirst() {
		return first;
	}

	public String getSecond() {
		return second;
	}

	// 读写函数
	public void readFields(DataInput in) throws IOException {
		String[] strings = WritableUtils.readStringArray(in);
		first = strings[0];
		second = strings[1];
	}

	public void write(DataOutput out) throws IOException {
		String[] strings = new String[] { first, second };
		WritableUtils.writeStringArray(out, strings);
	}


	// 覆写如下的函数
	@Override
	public String toString() {							// 将 StringPair 对象转为 String 类型对象
		final StringBuilder sb = new StringBuilder();
		sb.append(first + " " + second);
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		StringPair that = (StringPair) o;

		if (first != null ? !first.equals(that.first) : that.first != null)
			return false;
		if (second != null ? !second.equals(that.second) : that.second != null)
			return false;
		
		return true;
	}

	@Override
	public int hashCode() {
		int result = first != null ? first.hashCode() : 0;
		result = 31 * result + (second != null ? second.hashCode() : 0);
		return result;
		//return first.hashCode();
	}
	
	private int compare(String s1, String s2){
		if (s1 == null && s2 != null) {
			return -1;
		} else if (s1 != null && s2 == null) {
			return 1;
		} else if (s1 == null && s2 == null) {
			return 0; 
		} else {
			return s1.compareTo(s2);
		}
	}

	@Override
	public int compareTo(StringPair o) {
		int cmp = compare(first, o.getFirst());
		if(cmp != 0){
			return cmp;
		}
		return compare(second, o.getSecond());
	}	

}
