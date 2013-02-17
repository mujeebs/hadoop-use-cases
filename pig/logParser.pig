--$startString  Paramater specifying the start search string e.g. 'Starting Thread MerchantDisplayRequest'
--$endString    Parameter specify the end search string      e.g. 'Completed MerchantDisplayRequest'

register file:/home/mb/mypig/trunk/contrib/piggybank/java/piggybank.jar
register file:/usr/lib/pig/joda-time-2.1.jar;
DEFINE CustomFormatDiffDate org.apache.pig.piggybank.evaluation.datetime.CustomFormatDiffDate();
main_extract  = LOAD '$input' USING TextLoader as (line:chararray);
base_log = FOREACH main_extract GENERATE 
flatten (REGEX_EXTRACT_ALL(line, '^([\\w-]+\\s[\\w:]+[,]\\d{3}).*(\\sTID:).*(\\d{6,}).*([:]\\s[$startString|$endString].*).*'))
    as (
      dt:        chararray,
      tid:       chararray,
      threadId:  chararray,
      other:     chararray
    );
filtered_start = FILTER base_log BY other matches '.*($startString).*';
filtered_end = FILTER base_log BY other matches '.*($endString).*';
filtered_join = JOIN filtered_start BY threadId, filtered_end by threadId;
final_set = FOREACH filtered_join GENERATE filtered_start::threadId, CustomFormatDiffDate(filtered_end::dt, filtered_start::dt, 'yyyy-MM-dd HH:mm:ss,SSS') as dt;
STORE final_set into '$output';
