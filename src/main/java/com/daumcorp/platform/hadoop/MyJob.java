package com.daumcorp.platform.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class MyJob extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Logger logger = LoggerFactory.getLogger(getClass());
		private SimpleDateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value: 223.62.178.24 104495 51T1711SWMe - - [27/Aug/2013:00:00:20 +0900] "GET /_iphone/cafe.do?cmd=article&versionCode=20&grpid=HL9t&cafenode=cafe346&fldid=5WW8&dataid=1274&isRecent=&prevNextType=&regdt=&qacurrentnum=0&qalistnum=50&favorMode=&favorViewCnt=&favorShrtCmtCnt=&isQnaAnswerSummary=false&listnum=50 HTTP/1.1" 200 4240 "-" "DaumMobileApp (SHV-E160S; U; Android 2.3.6|10; ko-kr) DaumCafe/20"
			String[] logs = value.toString().split(" ");
			String reqUrl = logs[8];
			if (!StringUtils.contains(reqUrl, "cmd=article&") && !StringUtils.contains(reqUrl, "profile/article.do")
					&& !StringUtils.contains(reqUrl, "newsfeed/article.do")) {
				return;
			}

			String encUserid = logs[2];
			String userid = parseUserid(encUserid);
			if ("8GLAx".equals(userid)) {
				// qa test userid
				return;
			}

			String dateStr = logs[5];
			try {
				Date date = dateFormat.parse(dateStr);
				context.write(new Text(userid), new LongWritable(date.getTime()));
			} catch (ParseException e) {
				logger.error("parse error: {}", dateStr);
			}
		}

		private String parseUserid(String encUserid) {
			int idLength = NumberUtils.toInt(encUserid.substring(0, 1));
			int idStartIndex = encUserid.length() - idLength;
			return encUserid.substring(idStartIndex);
		}

	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long maxReadingDuration = NumberUtils.toLong(context.getConfiguration().get("max.reading.duration"), 30 * 1000);

			long lastTime = 0;
			int viewCount = 0;
			int sessionTime = 0;
			List<Long> logTimes = Lists.newArrayList();
			for (LongWritable val : values) {
				logTimes.add(val.get());
			}
			Collections.sort(logTimes);

			for (Long val : logTimes) {
				long logTime = val;
				if (lastTime == 0) {
					viewCount++;
					lastTime = logTime;
				} else if (logTime - lastTime < maxReadingDuration) {
					viewCount++;
					sessionTime += logTime - lastTime;
					lastTime = logTime;
				} else {
					if (viewCount > 1) {
						sessionTime += logTime - lastTime;
					}
					context.write(key, new Text(viewCount + "\t" + sessionTime));

					// 세션 초기화
					sessionTime = 0;
					viewCount = 1;
					lastTime = logTime;
				}
			}

			if (lastTime > 0) {
				context.write(key, new Text(viewCount + "\t" + sessionTime));
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = new Job(conf, "anal mobile app pv");
		job.setJarByClass(MyJob.class);

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyJob(), args);

		System.exit(res);
	}

}
