/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package equijoin;

/**
 *
 * @author Prem
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class equijoin {
    public static String t1Name = "";
	public static String t2Name = "";

	public static class MapClass extends Mapper<Text, Text, Text, Text> {

		private Text joinCol = new Text();
		private Text val = new Text();

		public void map(Text k, Text v, Context con)
				throws IOException, InterruptedException {

			String line = k.toString();
			String[] lineArr = k.toString().split(",");

			if (line != null) {
				if (t1Name.isEmpty())
					t1Name = lineArr[0];
				else {
					if (!t1Name.equals(lineArr[0])) {
						t2Name = lineArr[0];
					}
				}
			}

			joinCol.set(lineArr[1]);
			val.set(line);
			con.write(joinCol, val);

		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public Text joinResult = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<String> tab1 = new ArrayList<String>();
			ArrayList<String> tab2 = new ArrayList<String>();

			for (Text i : values) {
				String[] s = i.toString().split(",");
				if (s != null && s[0].trim().equals(t1Name)) {
					tab1.add(i.toString());
				} else if (s != null && s[0].trim().equals(t2Name)) {
					tab2.add(i.toString());
				}

			}

			String strData = "";
			for (int i = 0; i < tab1.size(); i++) {

				for (int j = 0; j < tab2.size(); j++) {

					strData = tab1.get(i) + ", " + tab2.get(j);
					joinResult.set(strData);
					context.write(null, joinResult);
					joinResult.clear();
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "equiJoin");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
