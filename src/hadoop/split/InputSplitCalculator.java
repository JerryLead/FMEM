package hadoop.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import profile.job.Job;
import profile.profiler.SingleJobProfiler;


public class InputSplitCalculator {
	// hdfsPath is comma separated paths
	public static List<InputSplit> getSplits(profile.commons.configuration.Configuration conf) {

		JobID jobId = new JobID();
		org.apache.hadoop.conf.Configuration jobConf = new org.apache.hadoop.conf.Configuration(
				false);

		for (Entry<String, String> entry : conf.getAllConfs())
			jobConf.set(entry.getKey(), entry.getValue());

		try {

			FileInputFormat fif = new TextInputFormat();

			JobContext jobContext = new JobContext(jobConf, jobId);
			List<InputSplit> inputSplits = fif.getSplits(jobContext);
			
			return inputSplits;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static List<Long> getSplitsLength(profile.commons.configuration.Configuration conf) {

		List<Long> lenList = new ArrayList<Long>();

		try {
			for (InputSplit is : getSplits(conf))
				lenList.add(is.getLength());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lenList;
	}

	public static void main(String[] args) {
		String jobId = "job_201210172333_0008";
		String hostname = "m105";

		boolean needMetrics = true; // going to analyze task
									// counters/metrics/jvm?
		int sampleMapperNum = 1;
		int sampleReducerNum = 1;

		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId,
				needMetrics, sampleMapperNum, sampleReducerNum);

		Job job = profiler.profile();

		profile.commons.configuration.Configuration newConf = job.getJobConfiguration();

		long minSplit = 256 * 1024 * 1024l;
		long maxSplit = Long.MAX_VALUE;

		newConf.set("mapred.input.dir", "hdfs://m105:9000/LijieXu/terasort-4GB");
		newConf.set("mapred.min.split.size", String.valueOf(minSplit));
		newConf.set("mapred.max.split.size", String.valueOf(maxSplit));

		List<InputSplit> inputSplits = getSplits(newConf);

		try {
			if (inputSplits != null) {
				for (InputSplit is : inputSplits) {
					System.out.println(is.getLength());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
