package comp9313.lab5;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 

 
public class BooleanInvertedList {
     
    /* 
     * mapper 阶段
     * Input: <docID, words> 
     * Output: <(word, docID), docID>
     */
    public static class BILMapper extends Mapper<Object, Text, StringPair, Text> { 
        
        // 声明 StringPair 对象，用来存放 (key, value)
        private StringPair pair = new StringPair();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");             
            
            // 获取文件名
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // 遍历每个 word， 作为 key的一部分
            while (itr.hasMoreTokens()) {
                // 形成 StringPair = (key, filename)
                pair.set(itr.nextToken().toLowerCase(), fileName);
                // 形成 <key,value> = <(word, docID), docID>
                context.write(pair, new Text(fileName));
            }
        }       
    }
    
    /*
     * 因为我们还是按照 key 进行划分的，所以需要自行编写 Partitioner 类 
     * 目的是让 mapper 输出的 pair，知道自己会被发送到哪里
     * 主要是覆写 hashcode()
     */
    public static class BILPartitioner extends Partitioner<StringPair, Text>{
        
        // 在我们做 partition 的时候，还是将相同的 word 送到一个reducer中
        public int getPartition(StringPair key, Text value, int numPartitions) { 
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    
    // 控制 reduce() 函数是按照哪个 key 进行分组
    public static class BILGroupingComparator extends WritableComparator{
    	
    	protected BILGroupingComparator(){
    		super(StringPair.class, true);
    	}
    	
    	public int compare(WritableComparable wc1, WritableComparable wc2){
    		
    		StringPair pair = (StringPair) wc1;
    		StringPair pair2 = (StringPair) wc2;    		
    		
    		return pair.getFirst().compareTo(pair2.getFirst());
    	}
    }
 	
 	/* 
     * reducer 阶段
 	 * Input: <(word, docID)), list(docID)>
 	 * Output: <Word, [docID, docID, ...]>
 	 */
    public static class BILReducer extends Reducer<StringPair, Text, Text, Text> {
  
         public void reduce(StringPair key, Iterable<Text> values, Context context)
                 throws IOException, InterruptedException {
        	     		
        	 String list = "";                             // 存储 filename 字符串
        	 Set<String> files = new HashSet<String>();    // 存储 filename 的集合
        	 for(Text fName : values){                     // 遍历每个 filename
        		 String name = fName.toString();
        		 if(!files.contains(name)){                // 排除相同的 filename  			 
        			 list += name + " ";                   // filename 连接
        			 files.add(name);
        		 }
        	 }        	 
        	 list = list.trim();                           // 删掉开头和结尾的 space 
             // 输出 <key, value> = <word, [filename, filename, ...]>
        	 context.write(new Text(key.getFirst()), new Text(list));
         }
    }
     
     
    public static void main(String[] args) throws Exception {       
         
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "boolean inverted list");
        job.setJarByClass(BooleanInvertedList.class);
        job.setMapperClass(BILMapper.class);
        job.setReducerClass(BILReducer.class);
        //either add this partitioner, or override the hashCode() function in StringPair
        job.setPartitionerClass(BILPartitioner.class);
        // 设置 mapper 的输出数据类型
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(Text.class);
        // 设置 reducer 的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setGroupingComparatorClass(BILGroupingComparator.class);
        
        job.setNumReduceTasks(2);       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


