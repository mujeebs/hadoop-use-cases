/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.piggybank.evaluation.datetime;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>long CustomFormatDiffDate(String date1, String date2, String format)</code>.</dd>
* <dt><b>Input:</b></dt>
* <dd><code>date1: string in format</code>.</dd>
* <dd><code>date2: string in format</code>.</dd>
* <dd><code>format: string custom format in which 2 dates are provided</code>.</dd>
* <dt><b>Output:</b></dt>
* <dd><code>(date1-date2) in millisecond</code>.</dd>
* </dl>
*/

public class CustomFormatDiffDate extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException
    {
        if (input == null || input.size() != 3 || input.get(0)==null || input.get(1)==null || input.get(2)==null) {
            String msg = "CustomFormatDiffDate : 3 Parameters are expected (Date 1, Date 2, Date format).";
            throw new IOException(msg);
        }

        // Set the time to default or the output is in UTC
        DateTimeZone.setDefault(DateTimeZone.UTC);

        String strDate1 = input.get(0).toString();
        String strDate2 = input.get(1).toString();
        String format = input.get(2).toString();

        DateFormat df = new SimpleDateFormat(format);
        
        Date date1 = null;
        Date date2 = null;
        try {
            date1 = df.parse(strDate1);
            date2 = df.parse(strDate2);
        } catch (ParseException e) {
            String msg = "DiffDate : Parameters have to be string in " +  format;
            warn(msg, PigWarning.UDF_WARNING_1);
            return null;
        }
        return (long)((date1.getTime() - date2.getTime())); 
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
}
