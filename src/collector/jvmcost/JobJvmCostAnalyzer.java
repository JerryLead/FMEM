package collector.jvmcost;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JobJvmCostAnalyzer {
	private String jobId;
	private int split;
	private int xmx;
	private int xms;
	private int ismb;
	private boolean reuse;
	private int reducers;
	
	private Capacity eCap;
	private Capacity realCap;
	
	public void analyzeMaxMinValue(File input, File output) {
		BufferedReader reader;
		PrintWriter mWriter;
		PrintWriter rWriter;
		
		try {
			File mapFile = new File(output, "realMapper.txt");
			File reduceFile = new File(output, "realReducer.txt");
			
			if(!mapFile.getParentFile().exists())
				mapFile.getParentFile().mkdirs();
			mWriter = new PrintWriter(new BufferedWriter(new FileWriter(mapFile)));
			rWriter = new PrintWriter(new BufferedWriter(new FileWriter(reduceFile)));
			
			mWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" + "Bytes" + "\t"
					+ "nOU" + "\t" + "mOU" + "\t" + "xOU" + "\t" + "OGC" + "\t" + "OGCMX" + "\t" 
					+ "nNGU" + "\t" + "mNGU" + "\t" + "xNGU" + "\t" + "NGC" + "\t" + "NGCMX" + "\t" 
					+ "nEdenU" + "\t" + "xEdenU" + "\t" + "EdenC" + "\t"
					+ "xS0U" + "\t" + "xS1U" + "\t" + "S0C" + "\t"
					+ "nHeapU" + "\t" + "xHeapU" + "\t" 
					+ "nRSS" + "\t" + "xRSS" + "\t"	
					+ "mYGC" + "\t" + "mFGC" + "\t" + "mYGCT" + "\t" + "mFGCT" + "\t" + "mTime" + "\t" + "jobId");
			rWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t"
					+ "nOU" + "\t" + "mOU" + "\t" + "xOU" + "\t" + "OGC" + "\t" + "OGCMX" + "\t" 
					+ "nNGU" + "\t" + "mNGU" + "\t" + "xNGU" + "\t" + "NGC" + "\t" + "NGCMX" + "\t" 
					+ "nEdenU" + "\t" + "xEdenU" + "\t" + "EdenC" + "\t"
					+ "nS0U" + "\t" + "xS0U" + "\t" + "S0C" + "\t"
					+ "nS1U" + "\t" + "xS1U" + "\t" + "S1C" + "\t"
					+ "nHeapU" + "\t" + "xHeapU" + "\t" 
					+ "nRSS" + "\t" + "xRSS" + "\t"	
					+ "nRecs" + "\t" + "xRecs" + "\t"
					+ "mYGC" + "\t" + "mFGC" + "\t" + "mYGCT" + "\t" + "mFGCT" + "\t" + "mTime" + "\t" + "jobId");

			reader = new BufferedReader(new FileReader(input));
			String line = reader.readLine();
			while(true) {
		
				if(line.contains("job_")) {
					List<MapperStat> mapList = new ArrayList<MapperStat>();
					
					List<ReducerStat> reduceList = new ArrayList<ReducerStat>();
					//-----------job_201211071045_0001 NewWordCountForWikipedia 256MB-----------
					jobId = line.substring(line.indexOf("job_"), line.indexOf(' '));
					
					//[Xmx]1000m [Xms]250m [ismb]200m [Reuse]false [reducers]16 [split] 256
					String attributes = reader.readLine();
					String[] params = attributes.replaceAll("\\[[^\\]]*\\]", "").split(" ");
				
					xmx = Integer.parseInt(params[0].substring(0, params[0].lastIndexOf('m')));
					xms = Integer.parseInt(params[1].substring(0, params[1].lastIndexOf('m')));
					ismb = Integer.parseInt(params[2].substring(0, params[2].lastIndexOf('m')));
					reuse = Boolean.parseBoolean(params[3]);
					reducers = Integer.parseInt(params[4]);
					split = Integer.parseInt(params[5]);
					
					//--------------------------Estimated/Real Capacity--------------------------
					//System.out.println(reader.readLine());
					reader.readLine();
					
					//MX	PGCMX	NGCMX	NGC	S0C	S1C	EC	OGCMX	OGC	PGCMX	PGC
					//System.out.println(reader.readLine().substring(3));
					reader.readLine();
					
					//4000	82.0	1333.3	83.5	10.4	10.4	62.6	2666.7	167.1	82.0	20.8
					String estimatedMX = reader.readLine();
					//int loc = estimatedMX.indexOf('\t');
					//System.out.println(estimatedMX.substring(loc + 1));
					eCap = new Capacity(estimatedMX.split("\t"));
					
					//4000	82.0	1333.3	83.1	10.4	10.4	62.4	2666.7	166.3	82.0	20.8	
					estimatedMX = reader.readLine();
					//loc = estimatedMX.indexOf('\t');
					//System.out.println(estimatedMX.substring(loc + 1));
					if(estimatedMX.equals("null")) {
						
						while(true) {
							line = reader.readLine();
							if(line == null) {
								reader.close();
								mWriter.close();
								rWriter.close();
								return;
							}
							else if(line.contains("job_"))
								break;
						}
						continue;
					}
					realCap = new Capacity(estimatedMX.split("\t"));
					
					//--------------------------mapper statitics--------------------------
					//System.out.println(reader.readLine());
					reader.readLine();
					
					//OU	EdenU	S0U	S1U	HeapU	Bytes	Index
					String indexLine = reader.readLine();
					//System.out.println("OU\tEdenU\tS0U\tS1U\tHeapU\tBytes");
					
					while(true) {
						line = reader.readLine();
						//--------------------------reducer statitics--------------------------
						if(line.contains("reducer")) {	
							break;
						}
						String[] p = line.split("\t");
						MapperStat ms = new MapperStat(p);
						if(mapList.isEmpty())
							mapList.add(ms);
						else if(ms.Bytes != mapList.get(0).Bytes) {
							outputMapperMaxMin(mapList, mWriter);
							mapList.clear();
							mapList.add(ms);
						}
						else
							mapList.add(ms);
						
					}
					
					outputMapperMaxMin(mapList, mWriter);
					//--------------------------reducer statitics--------------------------
					//System.out.println(line);
					//HeapU	OU	EdenU	S0U	S1U	Records	Index
					indexLine = reader.readLine();
					//System.out.println(indexLine.substring(0, indexLine.lastIndexOf('\t')));
					
					while(true) {
						line = reader.readLine();
						while(line != null && line.isEmpty())
							line = reader.readLine();
						if(line == null) {
							outputReducerMaxMin(reduceList, rWriter);
							reader.close();
							mWriter.close();
							rWriter.close();
							return;
						}
							
						if(line.contains("job_")) 
							break;
						
						String[] p = line.split("\t");
						ReducerStat ms = new ReducerStat(p);
						reduceList.add(ms);
						
					}
					outputReducerMaxMin(reduceList, rWriter);
					//System.out.println();
				}
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private void outputMapperMaxMin(List<MapperStat> mapList, PrintWriter writer) {
		if(mapList.isEmpty())
			return;
		float xOU = 0;
		float xEdenU = 0;
		float xNGU = 0;
		float xS0U = 0;
		float xS1U = 0;
		float xHeapU = 0;
		int xRSS = 0;
		
		float nOU = Float.MAX_VALUE;
		float nEdenU = Float.MAX_VALUE;
		float nNGU = Float.MAX_VALUE;
		float nS0U = Float.MAX_VALUE;
		float nS1U = Float.MAX_VALUE;
		float nHeapU = Float.MAX_VALUE;
		int nRSS = Integer.MAX_VALUE;
		
		//int medium = mapList.size() / 2;
		//int mYGC = mapList.get(medium).YGC;
		//int mFGC = mapList.get(medium).FGC;
		//int mTime = mapList.get(medium).TimeS;
		
		int size = mapList.size();
		float OU[] = new float[size];
		float NGU[] = new float[size];
		int YGC[] = new int[size];
		int FGC[] = new int[size];
		float YGCT[] = new float[size];
		float FGCT[] = new float[size];
		int Time[] = new int[size];
		
	
		for(int i = 0; i < mapList.size(); i++) {
			MapperStat ms = mapList.get(i);
			
			xOU = Math.max(xOU, ms.OU);
			xEdenU = Math.max(xEdenU, ms.EdenU);
			xNGU = Math.max(xNGU, ms.NGU);
			xS0U = Math.max(xS0U, ms.S0U);
			xS1U = Math.max(xS1U, ms.S1U);
			xHeapU = Math.max(xHeapU, ms.HeapU);
			xRSS = Math.max(xRSS, ms.RSS);
			
			nOU = Math.min(nOU, ms.OU);
			nEdenU = Math.min(nEdenU, ms.EdenU);
			nNGU = Math.min(nNGU, ms.NGU);
			nS0U = Math.min(nS0U, ms.S0U);
			nS1U = Math.min(nS1U, ms.S1U);
			nHeapU = Math.min(nHeapU, ms.HeapU);
			nRSS = Math.min(nRSS, ms.RSS);
			
			OU[i] = ms.OU;
			NGU[i] = ms.NGU;
			YGC[i] = ms.YGC;
			FGC[i] = ms.FGC;
			YGCT[i] = ms.YGCT;
			FGCT[i] = ms.FGCT;
			Time[i] = ms.TimeS;
		}
		
		Arrays.sort(OU);
		Arrays.sort(NGU);
		Arrays.sort(YGC);
		Arrays.sort(FGC);
		Arrays.sort(YGCT);
		Arrays.sort(FGCT);
		Arrays.sort(Time);
		
		int medium = size / 2;
		float mOU = OU[medium];
		float mNGU = NGU[medium];
		int mYGC = YGC[medium];
		int mFGC = FGC[medium];
		float mYGCT = YGCT[medium];
		float mFGCT = FGCT[medium];
		int mTime = Time[medium];
		
		//System.out.println(nOU + "\t" + nEdenU + "\t" + nS0U + "\t" + nS1U + "\t" + nHeapU + "\t" + mapList.get(0).Bytes);
		//System.out.println(xOU + "\t" + xEdenU + "\t" + xS0U + "\t" + xS1U + "\t" + xHeapU + "\t" + mapList.get(0).Bytes);
		
		writer.println(split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + reducers + "\t" + mapList.get(0).Bytes + "\t"
				+ nOU + "\t" + mOU + "\t" + xOU + "\t" + eCap.getOGC() + "\t" + eCap.getOGCMX() + "\t" 
				+ nNGU + "\t" + mNGU + "\t" + xNGU + "\t" + eCap.getNGC() + "\t" + eCap.getNGCMX() + "\t" 
				+ nEdenU + "\t" + xEdenU + "\t" + eCap.getEC() + "\t"
				+ xS0U + "\t" + xS1U + "\t" + eCap.getS0C() + "\t"
				+ nHeapU + "\t" + xHeapU + "\t" 
				+ nRSS + "\t" + xRSS + "\t"
				+ mYGC + "\t" + mFGC + "\t" + mYGCT + "\t" + mFGCT + "\t" + mTime + "\t" + jobId);
	}

	private void outputReducerMaxMin(List<ReducerStat> reducerList, PrintWriter writer) {
		if(reducerList.isEmpty())
			return;
		float xOU = 0;
		float xEdenU = 0;
		float xNGU = 0;
		float xS0U = 0;
		float xS1U = 0;
		float xHeapU = 0;
		int xRecords = 0;
		int xRSS = 0;
		
		float nOU = Float.MAX_VALUE;
		float nEdenU = Float.MAX_VALUE;
		float nNGU = Float.MAX_VALUE;
		float nS0U = Float.MAX_VALUE;
		float nS1U = Float.MAX_VALUE;
		float nHeapU = Float.MAX_VALUE;
		int nRecords = Integer.MAX_VALUE;
		int nRSS = Integer.MAX_VALUE;
		
		//int nYGC = Integer.MAX_VALUE;
		//int nFGC = Integer.MAX_VALUE;
		
		//int medium = reducerList.size() / 2;
		//int mTime = reducerList.get(medium).TimeS;
		int size = reducerList.size();
		float OU[] = new float[size];
		float NGU[] = new float[size];
		int YGC[] = new int[size];
		int FGC[] = new int[size];
		float YGCT[] = new float[size];
		float FGCT[] = new float[size];
		int Time[] = new int[size];
		
		for(int i = 0; i < reducerList.size(); i++) {
			ReducerStat ms = reducerList.get(i);
			xOU = Math.max(xOU, ms.OU);
			xEdenU = Math.max(xEdenU, ms.EdenU);
			xNGU = Math.max(xNGU, ms.NGU);
			xS0U = Math.max(xS0U, ms.S0U);
			xS1U = Math.max(xS1U, ms.S1U);
			xHeapU = Math.max(xHeapU, ms.HeapU);
			xRecords = Math.max(xRecords, ms.Records);
			xRSS = Math.max(xRSS, ms.RSS);
			
			nOU = Math.min(nOU, ms.OU);
			nEdenU = Math.min(nEdenU, ms.EdenU);
			nNGU = Math.min(nNGU, ms.NGU);
			nS0U = Math.min(nS0U, ms.S0U);
			nS1U = Math.min(nS1U, ms.S1U);
			nHeapU = Math.min(nHeapU, ms.HeapU);
			nRecords = Math.min(nRecords, ms.Records);
			nRSS = Math.min(nRSS, ms.RSS);
			
			OU[i] = ms.OU;
			NGU[i] = ms.NGU;
			YGC[i] = ms.YGC;
			FGC[i] = ms.FGC;
			YGCT[i] = ms.YGCT;
			FGCT[i] = ms.FGCT;
			Time[i] = ms.TimeS;
			
		}
		
		Arrays.sort(OU);
		Arrays.sort(NGU);
		Arrays.sort(YGC);
		Arrays.sort(FGC);
		Arrays.sort(YGCT);
		Arrays.sort(FGCT);
		Arrays.sort(Time);
		
		int medium = size / 2;
		float mOU = OU[medium];
		float mNGU = NGU[medium];
		int mYGC = YGC[medium];
		int mFGC = FGC[medium];
		float mYGCT = YGCT[medium];
		float mFGCT = FGCT[medium];
		int mTime = Time[medium];
		
		//System.out.println(nOU + "\t" + nEdenU + "\t" + nS0U + "\t" + nS1U + "\t" + nHeapU + "\t" + reducerList.get(0).Records);
		//System.out.println(xOU + "\t" + xEdenU + "\t" + xS0U + "\t" + xS1U + "\t" + xHeapU + "\t" + reducerList.get(0).Records);
		writer.println(split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + reducers + "\t"
				+ nOU + "\t" + mOU + "\t" + xOU + "\t" + eCap.getOGC() + "\t" + eCap.getOGCMX() + "\t" 
				+ nNGU + "\t" + mNGU + "\t" + xNGU + "\t" + eCap.getNGC() + "\t" + eCap.getNGCMX() + "\t" 
				+ nEdenU + "\t" + xEdenU + "\t" + eCap.getEC() + "\t"
				+ nS0U + "\t" + xS0U + "\t" + eCap.getS0C() + "\t"
				+ nS1U + "\t" + xS1U + "\t" + eCap.getS1C() + "\t"
				+ nHeapU + "\t" + xHeapU + "\t" 
				+ nRSS + "\t" + xRSS + "\t"
				+ nRecords + "\t" + xRecords + "\t"
				+ mYGC + "\t" + mFGC + "\t" + mYGCT + "\t" + mFGCT + "\t" + mTime + "\t" + jobId);
	}

	
	public static void main(String[] args) {
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/RealJvmCost/taskJvmDetails.txt";
		//String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/RealJvmCost/";
		
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/RealJvmCost/taskJvmDetails.txt";
		//String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/RealJvmCost/";
		
		String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/BuildCompIndex-m36-r18-256MB/RealJvmCost/taskJvmDetails.txt";
		String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/NewExperiments/BuildCompIndex-m36-r18-256MB/RealJvmCost/";
		
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/TeraSort-256MB-m36-r18/RealJvmCost/taskJvmDetails.txt";
		//String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/Experiments/TeraSort-256MB-m36-r18/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/RealJvmCost/taskJvmDetails.txt";
		//String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/RealJvmCost/";
		//String perTaskJvmMaxMinDir = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/RealJvmCost/taskJvmDetails.txt";
		//String perJobJvmMaxMinFile = "/home/xulijie/MR-MEM/Experiments/Wiki-m36-r18-256MB/RealJvmCost/";
		
		
		File input = new File(perTaskJvmMaxMinDir);
		File output = new File(perJobJvmMaxMinFile);
		
		JobJvmCostAnalyzer analyzer = new JobJvmCostAnalyzer();
		analyzer.analyzeMaxMinValue(input, output);
		
		System.out.println("Finished!!!");
	}

	
}

class Capacity {
	int MX;
	float PGCMX;
	float NGCMX;
	float NGC;
	float S0C;
	float S1C;
	float EC;
	float OGCMX;
	float OGC;
	float PGC;
	
	//MX	NGCMX	NGC	S0C	S1C	EC	OGCMX	OGC	PGCMX	PGC
	public Capacity(String[] param) {
		MX = Integer.parseInt(param[0]);
		//PGCMX = Float.parseFloat(param[1]);
		NGCMX = Float.parseFloat(param[1]);
		NGC = Float.parseFloat(param[2]);
		S0C = Float.parseFloat(param[3]);
		S1C = Float.parseFloat(param[4]);
		EC = Float.parseFloat(param[5]);
		OGCMX = Float.parseFloat(param[6]);
		OGC = Float.parseFloat(param[7]);
		
		PGC = Float.parseFloat(param[9]);
	}

	public int getMX() {
		return MX;
	}

	public float getPGCMX() {
		return PGCMX;
	}

	public float getNGCMX() {
		return NGCMX;
	}

	public float getNGC() {
		return NGC;
	}

	public float getS0C() {
		return S0C;
	}

	public float getS1C() {
		return S1C;
	}

	public float getEC() {
		return EC;
	}

	public float getOGCMX() {
		return OGCMX;
	}

	public float getOGC() {
		return OGC;
	}

	public float getPGC() {
		return PGC;
	}
	
}

class MapperStat {
	float OU;
	float EdenU;
	float NGU;
	float S0U;
	float S1U;
	float HeapU;
	int Bytes;
	int YGC;
	float YGCT;
	int FGC;
	float FGCT;
	float GCT;
	int TimeS;
	int Index;
	int RSS;
	
	public MapperStat(String[] param) {
		OU = Float.parseFloat(param[0]);
		EdenU = Float.parseFloat(param[1]);
		NGU = Float.parseFloat(param[2]);
		S0U = Float.parseFloat(param[3]);
		S1U = Float.parseFloat(param[4]);
		HeapU = Float.parseFloat(param[5]);
		Bytes = Integer.parseInt(param[6]);
		YGC = Integer.parseInt(param[7]);
		YGCT = Float.parseFloat(param[8]);
		FGC = Integer.parseInt(param[9]);
		FGCT = Float.parseFloat(param[10]);
		GCT = Float.parseFloat(param[11]);
		Index = Integer.parseInt(param[12]);
		TimeS = Integer.parseInt(param[13]);
		RSS = Integer.parseInt(param[14]);
		
	}
}

class ReducerStat {
	float OU;
	float EdenU;
	float NGU;
	float S0U;
	float S1U;
	float HeapU;
	int Records;
	int YGC;
	float YGCT;
	int FGC;
	float FGCT;
	float GCT;
	int Index;
	int Shuffle;
	int Sort;
	int Reduce;
	int TimeS;
	int RSS;
	
	public ReducerStat(String[] param) {
		OU = Float.parseFloat(param[0]);
		EdenU = Float.parseFloat(param[1]);
		NGU = Float.parseFloat(param[2]);
		S0U = Float.parseFloat(param[3]);
		S1U = Float.parseFloat(param[4]);
		HeapU = Float.parseFloat(param[5]);
		Records = Integer.parseInt(param[6]);
		YGC = Integer.parseInt(param[7]);
		YGCT = Float.parseFloat(param[8]);
		FGC = Integer.parseInt(param[9]);
		FGCT = Float.parseFloat(param[10]);
		GCT = Float.parseFloat(param[11]);
		Index = Integer.parseInt(param[12]);
		Shuffle = Integer.parseInt(param[13]);
		Sort = Integer.parseInt(param[14]);
		Reduce = Integer.parseInt(param[15]);
		TimeS = Integer.parseInt(param[16]);
		RSS = Integer.parseInt(param[17]);
	}
}

