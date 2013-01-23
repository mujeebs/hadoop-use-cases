package logfileExtractor;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogFileReducer extends Reducer<Text, Text, Text, Text> {
	
	SimpleDateFormat df = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss,S");

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		boolean first = true;
		String startTimeStr = null;
		String endTimeStr = null;
		
		for (Text val:values) {
			if (first) {
				startTimeStr = val.toString();
				first = false;
			} else {
				endTimeStr = val.toString();
			}
		}
		if (startTimeStr != null && endTimeStr != null)
		try {
			Date bd = df.parse(startTimeStr);
			Date ed = df.parse(endTimeStr);
			//As values are not sorted we can get end time record before start time, so just take the abs
			long timeDiff = Math.abs(ed.getTime() - bd.getTime());
			context.write(key, new Text(Long.toString(timeDiff)));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
