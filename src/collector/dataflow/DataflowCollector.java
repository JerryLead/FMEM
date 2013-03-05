package collector.dataflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.List;


import profile.job.Job;
import profile.profiler.SingleJobProfiler;
import profile.task.mapper.Mapper;
import profile.task.mapper.Merge;
import profile.task.mapper.MergeInfo;
import profile.task.reducer.Reducer;

public class DataflowCollector {
	public static void main(String[] args) {
		String startJobId = "job_201212252123_0001";
		String hostname = "master";
		int iterateNum = 80;
		boolean useHistory = false;
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/uservisits_aggre-pig-256MB-2/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/RealJvmCost/";
		//String dataflowDir = "/home/xulijie/MR-MEM/Experiments/TeraSort-256MB-m36-r18/RealDataflow/";
		//String dataflowDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/RealDataflow/";
		//String dataflowDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/RealDataflow/";
		//String dataflowDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/RealDataflow/";
		String dataflowDir = "/home/xulijie/MR-MEM/NewExperiments/BuildCompIndex-m36-r18-256MB/RealDataflow/";
		
		
		
		String prefix = startJobId.substring(0, startJobId.length() - 4);
		int suffix = Integer.parseInt(startJobId.substring(startJobId.length() - 4));
		
		DecimalFormat nf = new DecimalFormat("0000");
		DataflowCollector analyzer = new DataflowCollector();
		
		String jobId;
		
		for(int i = 0; i < iterateNum; i++) {
			jobId = prefix + nf.format(suffix + i);		
			Job job = analyzer.profile(hostname, jobId, useHistory);
			if(job == null) {
				System.err.println("[" + jobId + "] is a failed job");
				continue;
			}
			try {
				analyzer.outputRealDataflow(jobId, job, dataflowDir);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("[" + jobId + "]" + "'s data flow has been writen into " + dataflowDir);
		}
	}

	public void outputRealDataflow(String jobId, Job job, String dataflowDir) throws IOException {
		File mDataOutputFile = new File(dataflowDir + jobId, "fDataMappers.txt");
		File rDataOutputFile = new File(dataflowDir + jobId, "fDataReducers.txt");
		
		
		if(!mDataOutputFile.getParentFile().exists()) 
			mDataOutputFile.getParentFile().mkdirs();
		
		PrintWriter mDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(mDataOutputFile)));
		PrintWriter rDataWriter = new PrintWriter(new BufferedWriter(new FileWriter(rDataOutputFile)));	
		
		List<Mapper> fMappers = job.getMapperList();
		List<Reducer> fReducers = job.getReducerList();
		
		displayMapDataTitle(mDataWriter);
		displayMapperDataResult(fMappers, mDataWriter);
		
		displayReduceDataTitle(rDataWriter);
		displayReducerDataResult(fReducers, rDataWriter);
		
		mDataWriter.close();
		rDataWriter.close();
	}

	private void displayMapDataTitle(PrintWriter mDataWriter) {
		mDataWriter.println(
			    "InMB"+ "\t"
				+ "InRec" + "\t"
				+ "OutMB" + "\t"
				+ "OutRec" + "\t"
				
				+ "RecBM" + "\t"
				+ "RawBM" + "\t"
				+ "CompBM" + "\t"
				+ "RecAM" + "\t"
				+ "RawAM" + "\t"
				+ "CompAM" + "\t"
				+ "SegN"
		);
	}
	
	private void displayMapperDataResult(List<Mapper> fMappers, PrintWriter mDataWriter) {
		
		int MB = 1024 * 1024;
		String f0 = "%1$-3.0f";
		//String f1 = "%1$-3.1f";
		String f2 = "%1$-3.1f";
		
		for(Mapper fMapper : fMappers) {
			Merge fMerge = fMapper.getMerge();
			List<MergeInfo> mergeInfoList = fMerge.getMergeInfoList();
			
			int segmentsNum;
			double recordsBeforeMerge = 0;
			double rawLengthBeforeMerge = 0;
			double compressedLengthBeforeMerge = 0;
			
			double recordsAfterMerge = 0;
			double rawLengthAfterMerge = 0;
			double compressedLengthAfterMerge = 0;
			
			for(MergeInfo mergeInfo : mergeInfoList) {
				recordsBeforeMerge += mergeInfo.getRecordsBeforeMerge();
				rawLengthBeforeMerge += mergeInfo.getRawLengthBeforeMerge();
				compressedLengthBeforeMerge += mergeInfo.getCompressedLengthBeforeMerge();
				
				recordsAfterMerge += mergeInfo.getRecordsAfterMerge();
				rawLengthAfterMerge += mergeInfo.getRawLengthAfterMerge();
				compressedLengthAfterMerge += mergeInfo.getCompressedLengthAfterMerge();
			}
			
			segmentsNum = mergeInfoList.get(0).getSegmentsNum();
			
			double map_input_bytes = fMapper.getMapperCounters().getMap_input_bytes();
			double map_input_records = fMapper.getMapperCounters().getMap_input_records();
			double map_output_bytes = fMapper.getMapperCounters().getMap_output_bytes();
			double map_output_records = fMapper.getMapperCounters().getMap_output_records();
			
			mDataWriter.println(
					String.format(f0, map_input_bytes / MB) + "\t"
					+ String.format(f2, map_input_records / MB) + "\t"
					+ String.format(f2, map_output_bytes / MB) + "\t"
					+ String.format(f2, map_output_records / MB) + "\t"
					
					+ String.format(f2, recordsBeforeMerge / MB) + "\t"
					+ String.format(f2, rawLengthBeforeMerge / MB) + "\t"
					+ String.format(f2, compressedLengthBeforeMerge / MB) + "\t"
					+ String.format(f2, recordsAfterMerge / MB) + "\t"
					+ String.format(f2, rawLengthAfterMerge / MB) + "\t"
					+ String.format(f2, compressedLengthAfterMerge / MB) + "\t"
					+ segmentsNum
					);
		}
		
	}

	private void displayReduceDataTitle(PrintWriter rDataWriter) {
		rDataWriter.println(
			"ShCompMB" + "\t"
			+ "ShRawMB" + "\t"
			+ "InRec" + "\t"
			+ "InputMB" + "\t"
			+ "OutRec" + "\t"
			+ "OutMB"
		);
		
	}

	private void displayReducerDataResult(List<Reducer> fReducers, PrintWriter rDataWriter) {
		String f1 = "%1$-3.1f";
		int MB = 1024 * 1024;
		for(Reducer fReducer : fReducers) {
			double Reduce_shuffle_bytes = fReducer.getReducerCounters().getReduce_shuffle_bytes();
			double Reduce_shuffle_raw_bytes = fReducer.getReducerCounters().getReduce_shuffle_raw_bytes();
			double Reduce_input_records = fReducer.getReducerCounters().getReduce_input_records();
			
			double Reduce_input_bytes = fReducer.getReduce().getInputBytes();
			double Reduce_output_records = fReducer.getReduce().getOutputKeyValuePairsNum();
			double Reduce_output_bytes = fReducer.getReduce().getOutputBytes();
			
			rDataWriter.println(
				String.format(f1, Reduce_shuffle_bytes / MB) + "\t"
				+ String.format(f1, Reduce_shuffle_raw_bytes / MB) + "\t"
				+ String.format(f1, Reduce_input_records / MB) + "\t"
				+ String.format(f1, Reduce_input_bytes / MB) + "\t"
				+ String.format(f1, Reduce_output_records / MB) + "\t"
				+ String.format(f1, Reduce_output_bytes / MB)
			);
		}
		
	}
	
	public Job profile(String hostname, String jobId, boolean useHistory) {
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, true, 0, 0);
		Job job = profiler.profile();
		return job;
	}
	

}
