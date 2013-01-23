package logfileExtractor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	private LogFileInputParser lip = new LogFileInputParser();
	
	public void map(LongWritable key, Text value, Context context) throws 
			IOException, InterruptedException
	{
		Configuration conf = context.getConfiguration();
		String startSearchThread = conf.get(LogFileMapReducer.START_SEARCH_VAR);
		String endSearchThread = conf.get(LogFileMapReducer.END_SEARCH_VAR);
		//System.out.println("START="+ startSearchThread);
		lip.parse(value, startSearchThread, endSearchThread);
		if (lip.isValidRecord()) {
			context.write(new Text(lip.getLogKey()), new Text(lip.getLogValue()));
		}
	}
}
