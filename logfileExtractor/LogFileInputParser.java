package hadoop-use-cases.logfileExtractor;

import org.apache.hadoop.io.Text;

//Helper class to filter input records
public class LogFileInputParser {

	private String logKey;
	private String logValue;
	private boolean validRecord = false; 
	
	public void parse(String logText, String startSearchThread, String endSearchThread) {
		
		if (logText.indexOf(startSearchThread) != -1 || logText.indexOf(endSearchThread) != -1) { 
			//Input value contains the desired start/finish thread
			validRecord = true;
			
			//Our key is going to be thread id which is starting at position 28 TID:123456:
			logKey = logText.substring(28, logText.indexOf(":", 28));
			logValue = logText.substring(0, 23);  //so get the date-time part as value
		} 
	}

	public boolean isValidRecord() {
		return validRecord;
	}

	public void parse(Text logText, String startSearchThread, String endSearchThread) {
		parse(logText.toString(), startSearchThread, endSearchThread);
	}
	
	public String getLogKey() {
		return logKey;
	}

	public String getLogValue() {
		return logValue;
	}

	
}
