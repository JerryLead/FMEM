package collector.all;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import verification.split.dataflow.SplitReducerDataflow;
import verification.split.jvmcost.SplitMapperRealJvmCost;
import verification.split.jvmcost.SplitReducerRealJvmCost;

public class RealJvmCostSmallSplitFilter {
	public static void main(String[] args) {
		String jobDir = "G:\\MR-MEM\\BigExperiments\\";
		//String jobDir = "/home/xulijie/MR-MEM/SampleExperiments/";
		
		//String jobName = "BigBuildInvertedIndex";
		//String jobName = "BigTeraSort-36GB";
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "BigTwitterInDegreeCount";
		//String jobName = "Big-uservisits_aggre-pig-50G";
		String jobName = "BigWiki-m36-r18";

		String realDataflowDir = jobDir + jobName + "\\RealDataflow\\";
		String realJvmCostDir = jobDir + jobName + "\\RealJvmCost\\";

		String mapperJvmCost = "realMapper.txt";
		String filteredMapper = "filterRealMapper.txt";
		
		String reducerDataflow = "realDataReducer.txt";
		String reducerJvmCost = "realReducer.txt";
		String filteredReducer = "filterRealReducer.txt";
		
		filterMapper(realJvmCostDir, mapperJvmCost, filteredMapper);
		filterReducer(realJvmCostDir, reducerJvmCost, filteredReducer, realDataflowDir + reducerDataflow);
		
		System.out.println("Finished");
	}

	private static void filterReducer(String realJvmCostDir, String reducerJvmCost, String filteredReducer, String dataflowName) {
		File input = new File(realJvmCostDir + reducerJvmCost);
		File output = new File(realJvmCostDir + filteredReducer);
		
		if(!output.exists())
			output.getParentFile().mkdirs();
		
		try {
			BufferedReader dfReader = new BufferedReader(new FileReader(new File(dataflowName)));
			String dfTitles = dfReader.readLine();
			String[] dfTitle = dfTitles.split("\t");
			
			
			BufferedReader jvmReader = new BufferedReader(new FileReader(input));
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output)));
			
			String titles = jvmReader.readLine();
			String[] title = titles.split("\t");
			writer.println("XMS" + "\t" + titles + "\t" + dfTitles);
			
			String line;
			while((line = jvmReader.readLine()) != null) {
				String dfLine = dfReader.readLine();
				String[] dfValue = dfLine.split("\t");
				SplitReducerDataflow dataflow = new SplitReducerDataflow(dfTitle, dfValue);
				
				
				String[] jvmValue = line.split("\t");
				SplitReducerRealJvmCost jvmCost = new SplitReducerRealJvmCost(title, jvmValue);
				int xms = 0;
				if(jvmCost.getXms() == jvmCost.getXmx())
					xms = 1;
				
				if(!dataflow.getJobId().equals(jvmCost.getJobId()))
					System.err.println(dataflow.getJobId() + "\t" + jvmCost.getJobId());
				writer.println(xms + "\t" + line + "\t" + dfLine);
				
			}
			
			dfReader.close();
			jvmReader.close();
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void filterMapper(String realJvmCostDir, String mapperJvmCost,
			String filteredMapper) {
		File input = new File(realJvmCostDir + mapperJvmCost);
		File output = new File(realJvmCostDir + filteredMapper);
		
		if(!output.exists())
			output.getParentFile().mkdirs();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(output)));
			
			String titles = reader.readLine();
			String[] title = titles.split("\t");
			writer.println("XMS" + "\t" + titles);
			
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				SplitMapperRealJvmCost jvmCost = new SplitMapperRealJvmCost(title, value);
				int xms = 0;
				if(jvmCost.getXms() == jvmCost.getXmx())
					xms = 1;
				if(jvmCost.getBytes() == jvmCost.getSplit())
					writer.println(xms + "\t" + line);
			}
			
			reader.close();
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
