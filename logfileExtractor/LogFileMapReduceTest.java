package logfileExtractor.test;

import java.util.ArrayList;
import java.util.List;

import logfileExtractor.LogFileMapReducer;
import logfileExtractor.LogFileMapper;
import logfileExtractor.LogFileReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class LogFileMapReduceTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;

	@Before
	  public void setUp() {
		LogFileMapper mapper = new LogFileMapper();
	    LogFileReducer reducer = new LogFileReducer();

	    Configuration conf = new Configuration();
		conf.set(LogFileMapReducer.START_SEARCH_VAR, "Starting Thread MerchantDetailsDisplayRequest");
		conf.set(LogFileMapReducer.END_SEARCH_VAR, "Completed MerchantDisplayRequest");

		mapDriver = MapDriver.newMapDriver(mapper);
		mapDriver.setConfiguration(conf);
		
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		reduceDriver.setConfiguration(conf);
	}
	
	@Test
	public void testMapperStartSearch() {
		Text logRec = new Text("2012-12-06 15:19:12,403 TID:123456: Starting Thread MerchantDetailsDisplayRequest " + 
				"from 190.8.6.0 <MerchantId=MBTestMerch complete=yes readonly=true>");
		mapDriver.withInputValue(logRec);
		mapDriver.withMapper(new LogFileMapper());
		mapDriver.withInput(new LongWritable(1L), logRec);
		
		mapDriver.withOutput(new Text("123456"), new Text("2012-12-06 15:19:12,403"));
		mapDriver.runTest();
	}

	@Test
	public void testMapperEndSearch() {
		Text logRec = new Text("2012-12-06 15:19:13,031 TID:123456: Completed MerchantDisplayRequest for 190.8.6.0"); 
		mapDriver.withInputValue(logRec);
		mapDriver.withMapper(new LogFileMapper());
		mapDriver.withInput(new LongWritable(1L), logRec);
		
		mapDriver.withOutput(new Text("123456"), new Text("2012-12-06 15:19:13,031"));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("2012-12-06 15:19:13,031"));
		values.add(new Text("2012-12-06 15:19:12,403"));
		reduceDriver.withInput(new Text("123456"), values);
		reduceDriver.withOutput(new Text("123456"), new Text("628"));
		reduceDriver.runTest();
	}
	
	@Test
	public void ignoreNonSearcheableThreads() {
		Text logRec = new Text("2012-12-06 15:19:12,453 TID:123456: MBean for source DisplaySystem,sub=Stats registered.");
		mapDriver.withInputValue(logRec);
		mapDriver.withMapper(new LogFileMapper());
		mapDriver.withInput(new LongWritable(1L), logRec);
		
		mapDriver.runTest();
	}
}
