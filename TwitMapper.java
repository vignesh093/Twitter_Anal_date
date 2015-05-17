import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TwitMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> 
{
	public void map(LongWritable key,Text value,Context context)  throws IOException,InterruptedException
	{
		String my_time="hi";
		int my_hour=0;
		if(key.get() !=0)
		{
		String s=value.toString();
		String[] s_spl=s.split(",");
		if(s_spl.length > 15)
		{
		my_time=s_spl[15];
		}
		if(my_time.length() > 9)
		{
		my_hour=Integer.parseInt(my_time.substring(9,11));
		}
		else
		System.out.println("my time value"+s);
		context.write(new IntWritable(my_hour), new IntWritable(1));
		}
	}

}
