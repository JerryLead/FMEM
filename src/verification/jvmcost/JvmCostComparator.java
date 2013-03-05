package verification.jvmcost;

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


public class JvmCostComparator {

	private String jobName;
	private String baseDir;
	private int splitSizeMB;
	private boolean isJobIdInEstiDir;
	
	
	public JvmCostComparator(String jobName, String baseDir, int splitSizeMB, boolean isJobIdInEstiDir) {
		this.jobName = jobName;
		this.baseDir = baseDir;
		this.splitSizeMB = splitSizeMB;
		this.isJobIdInEstiDir = isJobIdInEstiDir;
	}


	public void compareJvmCost() {
		String rMapperFile = "realMapper.txt";
		String rReducerFile = "realReducer.txt";

		String realJvmCostDir = baseDir + jobName + "/RealJvmCost/";
		String estiJvmCostDir = baseDir + jobName + "/estimatedJvmCost/";
		String compJvmCostDir = baseDir + jobName + "/compJvmCost/";
		
		List<MapperRealJvmCost> rMapperJvmCostList = readRealMapperJvmCost(realJvmCostDir + rMapperFile, splitSizeMB);
		List<ReducerRealJvmCost> rReducerJvmCostList = readRealReducerJvmCost(realJvmCostDir + rReducerFile);
		
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
			
			if(isJobIdInEstiDir) {
				File eDir = new File(estiJvmCostDir);
				for(File jobIdFile : eDir.listFiles()) {
					String jobIdName = jobIdFile.getName();
					
					List<MapperPredictedJvmCost> eMapperJvmCostList = readEstimatedMapperJvmCost(estiJvmCostDir + jobIdName + File.separator + eMapperFile);
					List<ReducerPredictedJvmCost> eReducerJvmCostList = readEstimatedReducerJvmCost(estiJvmCostDir + jobIdName + File.separator + eReducerFile);
					
					List<MapperCompJvmCost> cMapperJvmCostList = compareMapperJvmCost(rMapperJvmCostList, eMapperJvmCostList);
					List<ReducerCompJvmCost> cReducerJvmCostList = compareReducerJvmCost(rReducerJvmCostList, eReducerJvmCostList);
					
					try {
						displayCompResults(cMapperJvmCostList, cReducerJvmCostList, compJvmCostDir + jobIdName + File.separator + cMapperFile, 
								compJvmCostDir + jobIdName + File.separator + cReducerFile, jobIdName);
						evaluateJvmCostDiff(cMapperJvmCostList, cReducerJvmCostList, diffMapperWriter, diffReducerWriter, jobIdFile.getName());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			/*
			else {
				List<MapperPredictedJvmCost> eMapperJvmCostList = readEstimatedMapperJvmCost(estiJvmCostDir + eMapperFile);
				List<ReducerPredictedJvmCost> eReducerJvmCostList = readEstimatedReducerJvmCost(estiJvmCostDir + eReducerFile);
				
				List<MapperCompJvmCost> cMapperJvmCostList = compareMapperJvmCost(rMapperJvmCostList, eMapperJvmCostList);
				List<ReducerCompJvmCost> cReducerJvmCostList = compareReducerJvmCost(rReducerJvmCostList, eReducerJvmCostList);
				
				try {
					displayCompResults(cMapperJvmCostList, cReducerJvmCostList, compJvmCostDir + cMapperFile, compJvmCostDir + cReducerFile, );
					evaluateJvmCostDiff(cMapperJvmCostList, cReducerJvmCostList, diffMapperWriter, diffReducerWriter, "jobId");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			*/
			diffMapperWriter.close();
			diffReducerWriter.close();
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}


	private void printDiffTitle(PrintWriter diffMapperWriter, PrintWriter diffReducerWriter) {
		diffMapperWriter.println(
				"xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "xOUdf" + "\t" + "mNGUdf" + "\t" + "xNGUdf" + "\t" 
				+ "xHeapUdf" + "\t" + "RSSdf" + "\t"
				+ "xOUrt" + "\t" + "mNGUrt" + "\t" + "xNGUrt" + "\t"
				+ "xHeapUrt" + "\t" + "RSSrt" + "\t" + "eJobId" + "\t" + "rJobId" + "\t"
				+ "rxOU" + "\t" + "exOU" + "\t"
				+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t"
				+ "rxHeapU" + "\t" + "rxRSS" + "\t" + "exHeapU");
		
		diffReducerWriter.println(
				"xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "mOUdf" + "\t" + "xOUdf" + "\t" + "mNGUdf" + "\t" + "xNGUdf" + "\t" 
				+ "xHeapUdf" + "\t" + "RSSdf" + "\t"
				+ "mOUrt" + "\t" + "xOUrt" + "\t" + "mNGUrt" + "\t" + "xNGUrt" + "\t"
				+ "xHeapUrt" + "\t" + "RSSrt" + "\t" + "eJobId" + "\t" + "rJobId" + "\t"
				+ "rmOU" + "\t" + "rxOU" + "\t" + "exOU" + "\t"
				+ "rmNGU" + "\t" + "rxNGU" + "\t" + "exNGU" + "\t"
				+ "rxHeapU" + "\t" + "rxRSS" + "\t" + "exHeapU");
		
	}


	private void evaluateJvmCostDiff(List<MapperCompJvmCost> cMapperJvmCostList, List<ReducerCompJvmCost> cReducerJvmCostList,
			PrintWriter diffMapperWriter, PrintWriter diffReducerWriter, String jobId) {
		
		for(MapperCompJvmCost cJvmCost : cMapperJvmCostList) 
			diffMapperWriter.println(cJvmCost.toDiffString(jobId));
			
		for(ReducerCompJvmCost cJvmCost : cReducerJvmCostList) 
			diffReducerWriter.println(cJvmCost.toDiffString(jobId));
		
	}
	
	private void displayCompResults(List<MapperCompJvmCost> cMapperJvmCostList, List<ReducerCompJvmCost> cReducerJvmCostList, 
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
		cMapperWriter.println("xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" + "Bytes" + "\t" 
			
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
		
		for(MapperCompJvmCost cJvmCost : cMapperJvmCostList)
			cMapperWriter.println(cJvmCost);
		
		
		
		//cReducerWriter.println("--------------------------------Redcucer Comparison-------------------------------");
		cReducerWriter.println("xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t"
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
		
		for(ReducerCompJvmCost cJvmCost : cReducerJvmCostList) 
			cReducerWriter.println(cJvmCost);
		
		cMapperWriter.close();
		cReducerWriter.close();
		
		System.out.println("[" + jobName + "] JvmCost comparison finished");
		
	}


	public List<MapperCompJvmCost> compareMapperJvmCost(List<MapperRealJvmCost> rMapperJvmCostList, List<MapperPredictedJvmCost> eMapperJvmCostList) {
		List<MapperCompJvmCost> list = new ArrayList<MapperCompJvmCost>();
		Map<String, MapperRealJvmCost> map = new HashMap<String, MapperRealJvmCost>();
		
		for(MapperRealJvmCost jc : rMapperJvmCostList) {
			String key = jc.getXmx() + "|" + jc.getXms() + "|" + jc.getIsmb() + "|" + jc.getRN();
			map.put(key, jc);
		}
		
		for(MapperPredictedJvmCost eJvmCost : eMapperJvmCostList) {
			String key = eJvmCost.getXmx() + "|" + eJvmCost.getXms() + "|" + eJvmCost.getIsmb() + "|" + eJvmCost.getRN();
			MapperRealJvmCost rJvmCost = map.get(key);
			if(rJvmCost != null) {
				MapperCompJvmCost cJvmCost = new MapperCompJvmCost(rJvmCost, eJvmCost);
				list.add(cJvmCost);
			}
		}
		return list;
	}
	
	public List<ReducerCompJvmCost> compareReducerJvmCost(List<ReducerRealJvmCost> rReducerJvmCostList, List<ReducerPredictedJvmCost> eReducerJvmCostList) {
		List<ReducerCompJvmCost> list = new ArrayList<ReducerCompJvmCost>();
		Map<String, ReducerRealJvmCost> map = new HashMap<String, ReducerRealJvmCost>();
		
		for(ReducerRealJvmCost jc : rReducerJvmCostList) {
			String key = jc.getXmx() + "|" + jc.getXms() + "|" + jc.getIsmb() + "|" + jc.getRN();
			map.put(key, jc);
		}
		
		for(ReducerPredictedJvmCost eJvmCost : eReducerJvmCostList) {
			String key = eJvmCost.getXmx() + "|" + eJvmCost.getXms() + "|" + eJvmCost.getIsmb() + "|" + eJvmCost.getRN();
			ReducerRealJvmCost rJvmCost	= map.get(key);
			if(rJvmCost != null) {
				ReducerCompJvmCost cJvmCost = new ReducerCompJvmCost(rJvmCost, eJvmCost);
				list.add(cJvmCost);
			}
		}
		
		return list;
	}
	
	public List<MapperRealJvmCost> readRealMapperJvmCost(String dir, int splitSizeMB) {
		List<MapperRealJvmCost> list = new ArrayList<MapperRealJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				MapperRealJvmCost jvmCost = new MapperRealJvmCost(title, value);
				if(jvmCost.getBytes() == splitSizeMB)
					list.add(jvmCost);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	public List<ReducerRealJvmCost> readRealReducerJvmCost(String dir) {
		List<ReducerRealJvmCost> list = new ArrayList<ReducerRealJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				ReducerRealJvmCost jvmCost = new ReducerRealJvmCost(title, value);
				list.add(jvmCost);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	private List<MapperPredictedJvmCost> readEstimatedMapperJvmCost(String dir) {
		List<MapperPredictedJvmCost> list = new ArrayList<MapperPredictedJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				MapperPredictedJvmCost jvmCost = new MapperPredictedJvmCost(title, value);
				list.add(jvmCost);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}

	private List<ReducerPredictedJvmCost> readEstimatedReducerJvmCost(String dir) {
		List<ReducerPredictedJvmCost> list = new ArrayList<ReducerPredictedJvmCost>();
		
		File input = new File(dir);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(input));
			String[] title = reader.readLine().split("\t");
			String line;
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				ReducerPredictedJvmCost jvmCost = new ReducerPredictedJvmCost(title, value);
				list.add(jvmCost);
			}
			
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
		
		JvmCostComparator jvmComp = new JvmCostComparator(jobName, baseDir, splitSizeMB, isJobIdInEstiDir);
				
		jvmComp.compareJvmCost();
		
	}

}
