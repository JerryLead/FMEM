package collector.all;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Map.Entry;

import profile.commons.configuration.Configuration;
import profile.job.Job;
import profile.profiler.SingleJobProfiler;
import collector.dataflow.DataflowAnalyzer;
import collector.dataflow.DataflowCollector;
import collector.jvmcost.JobJvmCostAnalyzer;
import collector.jvmcost.JvmCostCollector;

public class DataAndMemoryCollector {

	public static void main(String[] args) {
		String startJobId = "job_201301281455_0001";
		String hostname = "master";
		int iterateNum = 192;
		//String hostname = "m105";
		//int iterateNum = 1;

		//String jobDir = "/home/xulijie/MR-MEM/BigExperiments/";
		String jobDir = "/home/xulijie/MR-MEM/SampleExperiments/";
		//String jobDir = "/home/xulijie/MR-MEM/Test/";
		//String jobName = "BuildCompIndex-m36-r18-256MB";
		//String jobName = "Wiki-m36-r18";
		//String jobName = "BigTwitterGraphReverser";
		//String jobName = "SampleTeraSort-1G";
		//String jobName = "SampleWikiWordCount-1G";
		//String jobName = "SampleBuildInvertedIndex-1G";
		//String jobName = "SampleTwitterGraphBidirectEdge";
		String jobName = "SampleTwitterInDegreeCount";
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "BigTwitterInDegreeCount";
		//String jobName = "BigTeraSort";
		//String jobName = "uservisits_aggre-pig-256MB";
		//String jobName = "SampleWordCountWiki";
		//String jobName = "BuildCompIndex-m36-r18";
		
		String realDataflowDir = jobDir + jobName + "/RealDataflow/";
		String realJvmCostDir = jobDir + jobName + "/RealJvmCost/";
		String confDir = jobDir + jobName + "/Conf/";
		String serializeDir = jobDir + jobName + "/serialJob/";
		
		String jvmLogName = "taskJvmDetails.txt";
		
		String prefix = startJobId.substring(0, startJobId.length() - 4);
		int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));
		
		DecimalFormat nf = new DecimalFormat("0000");

		String jobId;
		
		DataflowCollector dataCollector = new DataflowCollector();
		DataflowAnalyzer dataAnalyzer = new DataflowAnalyzer(realDataflowDir); // write title ahead
		
		JvmCostCollector jvmCollector = new JvmCostCollector();
		JobJvmCostAnalyzer jvmAnalyzer = new JobJvmCostAnalyzer();
		
		for(int i = 0; i < iterateNum; i++) {
			jobId = prefix + nf.format(suffix + i);
			Job job = null;
			try {
				job = profile(hostname, jobId);
				
				serialize(serializeDir, job, jobId);
			} catch(NullPointerException e) {
				e.printStackTrace();
				jvmAnalyzer.analyzeMaxMinValue(new File(realJvmCostDir, jvmLogName), new File(realJvmCostDir));
			}
			
			if(job == null) {
				System.err.println("[" + jobId + "] is a failed job");
				continue;
			}
			
			jvmCollector.profile(hostname, jobId, false);
			
			try {
				outputConf(job.getJobConfiguration(), confDir + jobId + ".conf");
				
				dataCollector.outputRealDataflow(jobId, job, realDataflowDir);
				dataAnalyzer.analyzeMediumMaxValue(realDataflowDir, job.getJobConfiguration(), jobId);
				System.out.println("[" + jobId + "]" + "'s data flow has been parsed");
				
				jvmCollector.outputPerTaskJvmMaxMinValues(realJvmCostDir, jobId, jvmLogName);
				
				System.out.println("[" + jobId + "]" + "'s jvm max-min usage has been parsed");
				System.out.println();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		dataAnalyzer.close();
		jvmAnalyzer.analyzeMaxMinValue(new File(realJvmCostDir, jvmLogName), new File(realJvmCostDir));
	}
	
	private static void serialize(String serializeDir, Job job, String jobId) {
		File jobFile = new File(serializeDir, jobId + ".out");
		if(!jobFile.getParentFile().exists())
			jobFile.getParentFile().mkdirs();
		
		try {
			FileOutputStream fos = new FileOutputStream(jobFile);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(job);
	        oos.flush();
	        oos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		
	}

	private static void outputConf(Configuration jobConfiguration, String confPath) {
		File confFile = new File(confPath);
		
		if(!confFile.getParentFile().exists())
			confFile.getParentFile().mkdirs();
		
		PrintWriter writer;
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(confFile)));
			for(Entry<String, String> entry : jobConfiguration.getAllConfs()) {
				writer.write(entry.getKey() + "\t" + entry.getValue());
				writer.write("\r\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	public static Job profile(String hostname, String jobId) {
		//Do not parse counters/metrics/jvm metrics.
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, true, 0, 0);
		Job job = profiler.profile();
		return job;
	}
}
