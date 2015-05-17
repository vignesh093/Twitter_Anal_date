import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TwitReducer extends Reducer<IntWritable,IntWritable,Text,IntWritable> {
	int max_val=0;
	HashMap<Integer,Integer> mymap=new HashMap<Integer,Integer>();
	public void reduce(IntWritable key,Iterable<IntWritable> value,Context context)
	{
		int count=0;
		for(IntWritable val: value)
		{
			count+=val.get();
		}
		mymap.put(count,key.get());
		if(count > max_val
				)
		{
			max_val=count;
		}
	}
	public void run(Context context) throws IOException, InterruptedException 
	{
		setup(context);
		while(context.nextKeyValue())
		{
			reduce(context.getCurrentKey(), context.getValues(), context);
		}
		System.out.println("max val"+max_val);
		int ave_hr=mymap.get(max_val);
		context.write(new Text("Average time when user tweets more"), new IntWritable(ave_hr));
	}

}
