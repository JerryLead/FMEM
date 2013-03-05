package collector.dataflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import profile.commons.configuration.Configuration;

import verification.dataflow.MapperDataflow;
import verification.dataflow.ReducerDataflow;

public class DataflowAnalyzer {
	private PrintWriter mWriter;
	private PrintWriter rWriter;
	
	public DataflowAnalyzer(String realDataflowDir) {
		File mapperOut = new File(realDataflowDir, "realDataMapper.txt");
		File reducerOut = new File(realDataflowDir, "realDataReducer.txt");
		
		if(!mapperOut.getParentFile().exists())
			mapperOut.getParentFile().mkdirs();
		
		try {
			mWriter = new PrintWriter(new BufferedWriter(new FileWriter(mapperOut)));
			rWriter = new PrintWriter(new BufferedWriter(new FileWriter(reducerOut)));
			
			mWriter.println(
					"split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t"
					+ "mInMB" + "\t" + "mInRec" + "\t" + "mOutMB" + "\t" + "mOutRec" + "\t"
					+ "mRecBM"  + "\t" + "mRawBM" + "\t" + "mCompBM" + "\t" +  "mRecAM" + "\t" + "mRawAM" + "\t" + "mCompAM" + "\t"
					+ "mSegN" + "\t" 
					+ "xInMB" + "\t" + "xInRec" + "\t" + "xOutMB" + "\t" + "xOutRec" + "\t" + "xRecBM" + "\t" 
					+ "xRawBM" + "\t" + "xCompBM" + "\t" + "xRecAM" + "\t" + "xRawAM" + "\t" + "xCompAM" + "\t" 
					+ "xSegN" + "\t" + "jobId"
			);
			
			rWriter.println(
					"split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
					+ "mShCompMB" + "\t" + "mShRawMB" + "\t" + "mInRec" + "\t" + "mInputMB" + "\t" + "mOutRec" + "\t" + "mOutMB" + "\t"
					+ "xShCompMB" + "\t" + "xShRawMB" + "\t" + "xInRec" + "\t" + "xInputMB" + "\t" + "xOutRec" + "\t" + "xOutMB" + "\t"
					+ "jobId"
			);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	

	public void analyzeMediumMaxValue(String dataflowDir, Configuration conf, String jobId) {
		MapperDataflow mDataflow = new MapperDataflow(dataflowDir + jobId + "/fDataMappers.txt");
		ReducerDataflow rDataflow = new ReducerDataflow(dataflowDir + jobId + "/fDataReducers.txt");
		
		outputMapperDataflow(mDataflow, mWriter, conf, jobId);
		outputReducerDataflow(rDataflow, rWriter, conf, jobId);
	}

	public void close() {
		mWriter.close();
		rWriter.close();
	}

	private void outputMapperDataflow(MapperDataflow mDataflow, PrintWriter mWriter, Configuration conf, String jobId) {
		mWriter.print(conf.getSplitSize()/1024/1024 + "\t" + conf.getXmx() + "\t" + conf.getXms() + "\t" + conf.getIo_sort_mb() + "\t" + conf.getMapred_reduce_tasks() + "\t");
		mWriter.print(mDataflow);
		mWriter.println("\t" + jobId);
		
	}


	private void outputReducerDataflow(ReducerDataflow rDataflow, PrintWriter rWriter, Configuration conf, String jobId) {
		rWriter.print(conf.getSplitSize()/1024/1024 + "\t" + conf.getXmx() + "\t" + conf.getXms() + "\t" + conf.getIo_sort_mb() + "\t" + conf.getMapred_reduce_tasks() + "\t");
		rWriter.print(rDataflow);
		rWriter.println("\t" + jobId);	
	}
}
