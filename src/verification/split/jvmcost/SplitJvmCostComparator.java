package verification.split.jvmcost;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SplitJvmCostComparator {

	private String compJobName;
	private String compBaseDir;
	
	private String bigJobName;
	private String bigBaseDir;

	public SplitJvmCostComparator(String jobName, String baseDir) {
		this.compJobName = jobName;
		this.compBaseDir = baseDir;
		
		this.bigJobName = jobName;
		this.bigBaseDir = baseDir;
	}

	public SplitJvmCostComparator(String compJobName, String compBaseDir, String bigJobName, String bigBaseDir) {
		this.compJobName = compJobName;
		this.compBaseDir = compBaseDir;
		
		this.bigJobName = bigJobName;
		this.bigBaseDir = bigBaseDir;
		
		//this.splitMB = splitMB;
	}

	public void compareJvmCost() {
		String rMapperFile = "realMapper.txt";
		String rReducerFile = "realReducer.txt";

		String realJvmCostDir = bigBaseDir + bigJobName + "/RealJvmCost/";
		String estiJvmCostDir = compBaseDir + compJobName + "/estimatedDM/";
		String compJvmCostDir = compBaseDir + compJobName + "/compJvmCost/";
		
		List<SplitMapperRealJvmCost> rMapperJvmCostList = readRealMapperJvmCost(realJvmCostDir + rMapperFile);
		List<SplitReducerRealJvmCost> rReducerJvmCostList = readRealReducerJvmCost(realJvmCostDir + rReducerFile);
		
		String eMapperFile = "eJvmMappers.txt";
		String eReducerFile = "eJvmReducers.txt";
		
		String cMapperFile = "compMappers.txt";
		String cReducerFile = "compReducers.txt";
		
		String diffMapper = "diffMappers.txt";
		String diffReducer = "diffReducers.txt";
		
		File diffMapperFile = new File(compJvmCostDir + diffMapper);
		File diffReducerFile = new File(compJvmCostDir + diffReducer);
		
		if(!diffMapperFile.getParentFile().exists()) 
			diffMapperFile.getParentFile().mkdirs();
		
		PrintWriter diffMapperWriter;
		PrintWriter diffReducerWriter;
		try {
			diffMapperWriter = new PrintWriter(new BufferedWriter(new FileWriter(diffMapperFile)));
			diffReducerWriter = new PrintWriter(new BufferedWriter(new FileWriter(diffReducerFile)));
			printDiffTitle(diffMapperWriter, diffReducerWriter);
			
			File eDir = new File(estiJvmCostDir);
			for(File jobIdFile : eDir.listFiles()) {
				String jobIdName = jobIdFile.getName();
				
				List<SplitMapperPredictedJvmCost> eMapperJvmCostList = readEstimatedMapperJvmCost(estiJvmCostDir + jobIdName + File.separator + eMapperFile);
				List<SplitReducerPredictedJvmCost> eReducerJvmCostList = readEstimatedReducerJvmCost(estiJvmCostDir + jobIdName + File.separator + eReducerFile);
				
				List<SplitMapperCompJvmCost> cMapperJvmCostList = compareMapperJvmCost(rMapperJvmCostList, eMapperJvmCostList);
				List<SplitReducerCompJvmCost> cReducerJvmCostList = compareReducerJvmCost(rReducerJvmCostList, eReducerJvmCostList);
				
				try {
					displayCompResults(cMapperJvmCostList, cReducerJvmCostList, compJvmCostDir + jobIdName + File.separator + cMapperFile, 
							compJvmCostDir + jobIdName + File.separator + cReducerFile, jobIdName);
					evaluateJvmCostDiff(cMapperJvmCostList, cReducerJvmCostList, diffMapperWriter, diffReducerWriter, jobIdFile.getName());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
				
			diffMapperWriter.close();
			diffReducerWriter.close();
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}


	private void printDiffTitle(PrintWriter diffMapperWriter, PrintWriter diffReducerWriter) {
		diffMapperWriter.println(
				"split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "xOUdf" + "\t" + "mNGUdf" + "\t" + "xNGUdf" + "\t" 
				+ "xHeapUdf" + "\t" + "RSSdf" + "\t"
				+ "xOUrt" + "\t" + "mNGUrt" + "\t" + "xNGUrt" + "\t"
				+ "xHeapUrt" + "\t" + "RSSrt" + "\t" + "eJobId" + "\t" + "rJobId" + "\t"
				+ "rxOU" + "\t" + "exOU" + "\t"
				+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t"
				+ "rxHeapU" + "\t" + "rxRSS" + "\t" + "exHeapU");
		
		diffReducerWriter.println(
				"split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "mOUdf" + "\t" + "xOUdf" + "\t" + "mNGUdf" + "\t" + "xNGUdf" + "\t" 
				+ "xHeapUdf" + "\t" + "RSSdf" + "\t"
				+ "mOUrt" + "\t" + "xOUrt" + "\t" + "mNGUrt" + "\t" + "xNGUrt" + "\t"
				+ "xHeapUrt" + "\t" + "RSSrt" + "\t" + "eJobId" + "\t" + "rJobId" + "\t"
				+ "rmOU" + "\t" + "rxOU" + "\t" + "exOU" + "\t"
				+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t"
				+ "rxHeapU" + "\t" + "rxRSS" + "\t" + "exHeapU");
		
	}


	private void evaluateJvmCostDiff(List<SplitMapperCompJvmCost> cMapperJvmCostList, List<SplitReducerCompJvmCost> cReducerJvmCostList,
			PrintWriter diffMapperWriter, PrintWriter diffReducerWriter, String jobId) {
		
		for(SplitMapperCompJvmCost cJvmCost : cMapperJvmCostList) 
			diffMapperWriter.println(cJvmCost.toDiffString(jobId));
			
		for(SplitReducerCompJvmCost cJvmCost : cReducerJvmCostList) 
			diffReducerWriter.println(cJvmCost.toDiffString(jobId));
		
	}
	
	private void displayCompResults(List<SplitMapperCompJvmCost> cMapperJvmCostList, List<SplitReducerCompJvmCost> cReducerJvmCostList, 
			String mapperFile, String reducerFile, String jobName) throws IOException {
		File mFile = new File(mapperFile);
		File rFile = new File(reducerFile);
		
		if(!mFile.getParentFile().exists()) 
			mFile.getParentFile().mkdirs();
		if(!rFile.getParentFile().exists())
			rFile.getParentFile().mkdirs();
		
		PrintWriter cMapperWriter = new PrintWriter(new FileWriter(mFile));
		PrintWriter cReducerWriter = new PrintWriter(new FileWriter(rFile));
		
		//cMapperWriter.println("--------------------------------Mapper Comparison--------------------------------");
		cMapperWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" + "Bytes" + "\t" 
			
			+ "rxOU" + "\t" + "exOU" + "\t" + "diff" + "\t"
			+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t" + "mDiff" + "\t" + "rDiff" + "\t"
			
			+ "rnOU" + "\t" + "enOU" + "\t" + "diff" + "\t" 
			+ "rnNGU" + "\t" + "enNGU" + "\t" + "diff" + "\t"
			
			+ "rnHeapU" + "\t" + "enHeapU" + "\t" + "diff" + "\t"
			+ "rxHeapU" + "\t" + "exHeapU" + "\t" + "diff" + "\t"
			+ "rnRSS" + "\t" + "rxRSS" + "\t" + "HRDiff" + "\t"
			
			+ "rnEdenU" + "\t" + "rxEdenU" + "\t"
			+ "rxS0U" + "\t" + "rxS1U" + "\t" 
			+ "enTempObj" + "\t" + "exTempObj" + "\t" + "enFix" + "\t" + "exFix" + "\t"
			+ "OGC" + "\t" + "OGCMX" + "\t" + "NGC" + "\t" + "NGCMX" + "\t" + "EdenC" + "\t" + "S0C" + "\t" 
			+ "mYGC" + "\t" + "mFGC" + "\t" + "mTime" + "\t" + "reason");
		
		for(SplitMapperCompJvmCost cJvmCost : cMapperJvmCostList)
			cMapperWriter.println(cJvmCost);
		
		
		
		//cReducerWriter.println("--------------------------------Redcucer Comparison-------------------------------");
		cReducerWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t"
			+ "rmOU" + "\t" + "rxOU" + "\t" + "exOU" + "\t" + "mDiff" + "\t" + "xDiff" + "\t"
			+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t" + "mDiff" + "\t" + "xDiff" + "\t"
			
			+ "rnOU" + "\t" + "enOU" + "\t" + "diff" + "\t"
			+ "rnNGU" + "\t" + "enNGU" + "\t" + "diff" + "\t"
			
			+ "rnHeapU" + "\t" + "enHeapU" + "\t" + "diff" + "\t"
			+ "rxHeapU" + "\t" + "exHeapU" + "\t" + "diff" + "\t"
			+ "rnRSS" + "\t" + "rxRSS" + "\t" + "HRDiff" + "\t"
			
			+ "IMSB" + "\t" + "MergB" + "\t" + "ShufMB" + "\t"
			+ "nRedIn" + "\t" + "xRedIn" + "\t"
			
			+ "rnEdenU" + "\t" + "rxEdenU" + "\t"
			+ "rnS0U" + "\t" + "rxS0U" + "\t" + "rnS1U" + "\t" + "rxS1U" + "\t" 
			+ "enSSTObj" + "\t" + "exSSTObj" + "\t" + "enRTObj" + "\t" + "exRTObj" + "\t" 
			+ "enFix" + "\t" + "exFix" + "\t"
			+ "OGC" + "\t" + "OGCMX" + "\t" + "NGC" + "\t" + "NGCMX" + "\t" + "EdenC" + "\t" + "S0C" + "\t" 
			+ "rnRecords" + "\t" + "rxRecords" + "\t" + "nYGC" + "\t" + "nFGC" + "\t" + "mTime" + "\t" + "reason");
		
		for(SplitReducerCompJvmCost cJvmCost : cReducerJvmCostList) 
			cReducerWriter.println(cJvmCost);
		
		cMapperWriter.close();
		cReducerWriter.close();
		
		System.out.println("[" + jobName + "] JvmCost comparison finished");
		
	}


	public List<SplitMapperCompJvmCost> compareMapperJvmCost(List<SplitMapperRealJvmCost> rMapperJvmCostList, List<SplitMapperPredictedJvmCost> eMapperJvmCostList) {
		List<SplitMapperCompJvmCost> list = new ArrayList<SplitMapperCompJvmCost>();
		Map<String, SplitMapperRealJvmCost> map = new HashMap<String, SplitMapperRealJvmCost>();
		
		for(SplitMapperRealJvmCost jc : rMapperJvmCostList) {
			String key = jc.getSplit() + "|" + jc.getXmx() + "|" + jc.getXms() + "|" + jc.getIsmb() + "|" + jc.getRN();
			map.put(key, jc);
		}
		
		for(SplitMapperPredictedJvmCost eJvmCost : eMapperJvmCostList) {
			String key = eJvmCost.getSplit() + "|" + eJvmCost.getXmx() + "|" + eJvmCost.getXms() + "|" + eJvmCost.getIsmb() + "|" + eJvmCost.getRN();
			SplitMapperRealJvmCost rJvmCost = map.get(key);
			if(rJvmCost != null) {
				SplitMapperCompJvmCost cJvmCost = new SplitMapperCompJvmCost(rJvmCost, eJvmCost);
				list.add(cJvmCost);
			}
		}
		return list;
	}
	
	public List<SplitReducerCompJvmCost> compareReducerJvmCost(List<SplitReducerRealJvmCost> rReducerJvmCostList, List<SplitReducerPredictedJvmCost> eReducerJvmCostList) {
		List<SplitReducerCompJvmCost> list = new ArrayList<SplitReducerCompJvmCost>();
		Map<String, SplitReducerRealJvmCost> map = new HashMap<String, SplitReducerRealJvmCost>();
		
		for(SplitReducerRealJvmCost jc : rReducerJvmCostList) {
			String key = jc.getSplit() + "|" + jc.getXmx() + "|" + jc.getXms() + "|" + jc.getIsmb() + "|" + jc.getRN();
			map.put(key, jc);
		}
		
		for(SplitReducerPredictedJvmCost eJvmCost : eReducerJvmCostList) {
			String key = eJvmCost.getSplit() + "|" + eJvmCost.getXmx() + "|" + eJvmCost.getXms() + "|" + eJvmCost.getIsmb() + "|" + eJvmCost.getRN();
			SplitReducerRealJvmCost rJvmCost = map.get(key);
			if(rJvmCost != null) {
				SplitReducerCompJvmCost cJvmCost = new SplitReducerCompJvmCost(rJvmCost, eJvmCost);
				list.add(cJvmCost);
			}
		}
		
		return list;
	}
	
	public List<SplitMapperRealJvmCost> readRealMapperJvmCost(String dir) {
		List<SplitMapperRealJvmCost> list = new ArrayList<SplitMapperRealJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				SplitMapperRealJvmCost jvmCost = new SplitMapperRealJvmCost(title, value);
				if(jvmCost.getBytes() == jvmCost.getSplit())
					list.add(jvmCost);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	public List<SplitReducerRealJvmCost> readRealReducerJvmCost(String dir) {
		List<SplitReducerRealJvmCost> list = new ArrayList<SplitReducerRealJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				SplitReducerRealJvmCost jvmCost = new SplitReducerRealJvmCost(title, value);
				list.add(jvmCost);
			}
			reader.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	private List<SplitMapperPredictedJvmCost> readEstimatedMapperJvmCost(String dir) {
		List<SplitMapperPredictedJvmCost> list = new ArrayList<SplitMapperPredictedJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				SplitMapperPredictedJvmCost jvmCost = new SplitMapperPredictedJvmCost(title, value);
				list.add(jvmCost);
			}
			reader.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}

	private List<SplitReducerPredictedJvmCost> readEstimatedReducerJvmCost(String dir) {
		List<SplitReducerPredictedJvmCost> list = new ArrayList<SplitReducerPredictedJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				SplitReducerPredictedJvmCost jvmCost = new SplitReducerPredictedJvmCost(title, value);
				list.add(jvmCost);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	public static void main(String[] args) throws IOException {
		//String realJvmCostDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/RealJvmCost/";
		//String estiJvmCostDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/estimatedJvmCost/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/compJvmCost/";
		
		//String realJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/RealJvmCost/";
		//String estiJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/estimatedJvmCost/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/compJvmCost/";
		
		//String realJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/RealJvmCost/";
		//String estiJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/estimatedJvmCost/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/compJvmCost/";
		
		
		//String realJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/RealJvmCost/";
		//String estiJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/estimatedJvmCost/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/compJvmCost/";
		String jobName = "BuildCompIndex-m36-r18-256MB";
		String baseDir = "/home/xulijie/MR-MEM/NewExperiments2/";
		int splitSizeMB = 256; //set the split size to filter the finished mappers
		boolean isJobIdInEstiDir = true;
		
		SplitJvmCostComparator jvmComp = new SplitJvmCostComparator(jobName, baseDir);
				
		jvmComp.compareJvmCost();
		
	}

}
