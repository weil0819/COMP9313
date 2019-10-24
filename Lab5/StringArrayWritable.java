package comp9313.lab5;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;

/*
 * Mapper/Reducer 的 key 如果是复杂类型，那么就需要在创建该类型的时候，继承 Writable 类
 */
public class StringArrayWritable extends ArrayWritable {
	
	// 构造函数-1： 空的
	public StringArrayWritable() {
		super(Text.class);
		set(new Text[]{});
	}

	// 构造函数-2： 从 String 数组构造 StringArrayWritable 对象
	public StringArrayWritable(ArrayList<String> array) {
		this();
		int n = array.size();
		Text [] elements = new Text[n];
		for(int i=0;i<n;i++){
			elements[i] = new Text(array.get(i));	// 数据中的每个元素作为一个对象
		}
		set(elements);
	}
	
	public StringArrayWritable(Text[] array) {
		this();
		set(array);
	}
	
	// 将 StringArrayWritable 转换为 String 数组
	public String[] toStrings(){
		Writable [] array = get();
		int n = array.length;
		
		String[] elements = new String[n];
		for(int i=0;i<n;i++){
			elements[i] = ((Text)array[i]).toString();
		}
		return elements;
	}
	
	//serialize the array, separate strings by ","
	// 将 StringArrayWritable 转换为一个 String 对象
	public String toString(){
		return StringUtils.join(toStrings(), " ");
	}
}
