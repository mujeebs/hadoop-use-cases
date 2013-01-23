package logfileExtractor;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogFileMapReducer extends Configured implements Tool {

	public static String START_SEARCH_VAR = "startSerachThread";
	public static String END_SEARCH_VAR = "endSearchThread";
	
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set(START_SEARCH_VAR, args[2]);
		conf.set(END_SEARCH_VAR, args[3]);
		
		Job job = new Job(conf, "LogFileMapReducer");
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("LogFileMapReducer");
		job.setMapperClass(LogFileMapper.class);
		job.setReducerClass(LogFileReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ?0:1);
		
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new LogFileMapReducer(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);
	}

}
