package comp9313.lab4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CoTermSynStripe {
	
	/*
	 * mapper 阶段
	 * Input： <docID, words>
	 * Output： <word1, (word2, count)>
	 */
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");			
						
			ArrayList<String> termArray = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				termArray.add(itr.nextToken().toLowerCase());
			}
			
			for(int i=0;i<termArray.size();i++){
				// 声明 MapWritable 对象，存放 value = (word, count)
				MapWritable record = new MapWritable();
				Text word1 = new Text(termArray.get(i));
				
				for(int j=i+1;j<termArray.size();j++){
					Text word2 = new Text(termArray.get(j));
					
					//consider the order of two words!
					if(termArray.get(i).compareTo(termArray.get(j)) < 0){
						if(record.containsKey(word2)){
							IntWritable count = (IntWritable)record.get(word2);
							count.set(count.get() + 1);	// 累加 count
							record.put(word2, count);
						}else{
							record.put(word2, new IntWritable(1));
						}
					}else{
						MapWritable ttmap = new MapWritable();
						ttmap.put(word1, new IntWritable(1));
						context.write(word2, ttmap);
					}
				}
				// 输出的 <key, value> = <word1, (word2, count)>
				// 其中，value = (word2, count) 是 MapWritable 对象
				context.write(word1, record); 
			}
		}		
	}

	/*
	 * combiner 阶段
	 * Input： <word1, (word2, count)>
	 * Output： <word1, (word2, #count)>
	 */
	public static class SynStripeCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {	
			
			MapWritable map = new MapWritable();
			
			for (MapWritable val : values) {
				Set<Entry<Writable, Writable> > sets = val.entrySet();
				for(Entry<Writable, Writable> entry: sets){
					Text word2 = (Text)entry.getKey();
					int count = ((IntWritable)entry.getValue()).get();
					if(map.containsKey(word2)){
						map.put(word2, new IntWritable(((IntWritable)map.get(word2)).get() + count));
					}else{
						map.put(word2, new IntWritable(count));
					}						
				}
			}
			// mapper 和 combiner 的输出是一样的
			context.write(key, map);
		}
	}
	
	/*
	 * reducer 阶段
	 * Input： <word1, (word2, count)>
	 * Output： <(word1, word2), count>
	 */
	public static class SynStripeReducer extends Reducer<Text, MapWritable, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable count = new IntWritable();

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			// map 存放的是 <word2, count>		
			HashMap<String, Integer> map = new HashMap<String, Integer>();

			for (MapWritable val : values) {
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				for (Entry<Writable, Writable> entry : sets) {
					String word2 = ((Text) entry.getKey()).toString();	// key 是 word2
					int num = ((IntWritable) entry.getValue()).get();	// value 是 count
					if (map.containsKey(word2)) {
						map.put(word2, map.get(word2).intValue() + num);
					} else {
						map.put(word2, new Integer(num));
					}
				}
			}

			// Do sorting, in order to make the result exactly the same as the pair approach
			// 对于 word1 来讲，它对应的所有 word2 进行排序
			Object[] sortkey = map.keySet().toArray();
			Arrays.sort(sortkey);
			for (int i = 0; i < sortkey.length; i++) {
				// word 是 Text 数据类型，所以字符串拼接
				word.set(key.toString() + " " + sortkey[i].toString());
				count.set(map.get(sortkey[i]));		// 设置 count
				context.write(word, count);
			}	
			
			//This does not do sort
			/*Set<Entry<String, Integer> > sets = map.entrySet();
			for(Entry<String, Integer> entry: sets){
				word.set(key.toString() + " " + entry.getKey());
				count.set(entry.getValue());
				context.write(word, count);
			}*/
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "term co-occurrence symmetric stripe");
		job.setJarByClass(CoTermSynStripe.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SynStripeCombiner.class);
		job.setReducerClass(SynStripeReducer.class);	
		// 因为 mapper 的输出 value 类型是特殊的，所以需要指定	
		job.setMapOutputValueClass(MapWritable.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
