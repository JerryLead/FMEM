package verification.split.dataflow;

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


public class SplitDataflowComparator {
	private Map<String, SplitMapperDataflow> mDfMap = new HashMap<String, SplitMapperDataflow>();
	private Map<String, SplitReducerDataflow> rDfMap = new HashMap<String, SplitReducerDataflow>();

	private String compJobName;
	private String compBaseDir;
	
	private String bigJobName;
	private String bigBaseDir;
	//private int[] splitMB;
	
	public SplitDataflowComparator(String jobName, String baseDir) {
		this.compJobName = jobName;
		this.compBaseDir = baseDir;
		
		this.bigJobName = jobName;
		this.bigBaseDir = baseDir;
	}

	public SplitDataflowComparator(String compJobName, String compBaseDir, String bigJobName, String bigBaseDir) {
		this.compJobName = compJobName;
		this.compBaseDir = compBaseDir;
		
		this.bigJobName = bigJobName;
		this.bigBaseDir = bigBaseDir;
		
		//this.splitMB = splitMB;
	}
	
	public void compareDataflow() {
		String realDataflowDir = bigBaseDir + bigJobName + "/RealDataflow/";
		String estimatedJvmCostDir = compBaseDir + compJobName + "/estimatedDM/";
		String compDataflow = compBaseDir + compJobName + "/compDataflow/";

		readFinishedDataflow(realDataflowDir);
		compDataflow(estimatedJvmCostDir, compDataflow);

		System.out.println("[" + compJobName + "] dataflow comparison finished");
	}
	
	
	private void compDataflow(String estimatedDMDir, String compDataflow) {
		File eDir = new File(estimatedDMDir);
		File compJobIdDir = new File(compDataflow);
		if (!compJobIdDir.exists())
			compJobIdDir.mkdirs();

		try {
			PrintWriter mWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(compJobIdDir, "compMappers.txt"))));
			PrintWriter rWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(compJobIdDir, "compReducers.txt"))));
			
			displayCompMapperTitle(mWriter);
			displayCompReducerTitle(rWriter);

			for (File jobIdFile : eDir.listFiles()) 
				compareDataflowEveryJob(jobIdFile, mWriter, rWriter);

			mWriter.close();
			rWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private void compareDataflowEveryJob(File jobIdFile, PrintWriter mWriter, PrintWriter rWriter) {
		File eDataMappersFile = new File(jobIdFile, "eDataMappers.txt");
		File eDataReducersFile = new File(jobIdFile, "eDataReducers.txt");
		
		List<SplitMapperDataflow> mDfList = new ArrayList<SplitMapperDataflow>();
		List<SplitReducerDataflow> rDfList = new ArrayList<SplitReducerDataflow>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(eDataMappersFile));	
			String[] title = reader.readLine().split("\t");
			String line;
			
			while((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				SplitMapperDataflow emDataflow = new SplitMapperDataflow(title, values);
				mDfList.add(emDataflow);
			}
			
			reader.close();
			
			reader = new BufferedReader(new FileReader(eDataReducersFile));	
			title = reader.readLine().split("\t");
			
			while((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				SplitReducerDataflow emDataflow = new SplitReducerDataflow(title, values);
				rDfList.add(emDataflow);
			}
			
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		for(SplitMapperDataflow emDataflow : mDfList) {
			emDataflow.setJobId(jobIdFile.getName());
			
			int split = emDataflow.getSplit();
			int xmx = emDataflow.getXmx();
			int xms = emDataflow.getXms();
			int ismb = emDataflow.getIsmb();
			int RN = emDataflow.getRN();

			String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
			String key = split + "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
			SplitMapperDataflow rmDataflow = mDfMap.get(key);

			if (emDataflow == null || rmDataflow == null) {
				//System.out.println(key);
			} else {
				displayCompMapperDataflow(rmDataflow, emDataflow, mWriter);
			}
		}	
		
		for(SplitReducerDataflow erDataflow : rDfList) {		
			erDataflow.setJobId(jobIdFile.getName());
			
			int split = erDataflow.getSplit();
			int xmx = erDataflow.getXmx();
			int xms = erDataflow.getXms();
			int ismb = erDataflow.getIsmb();
			int RN = erDataflow.getRN();

			String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
			String key = split + "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
			SplitReducerDataflow rrDataflow = rDfMap.get(key);

			if (erDataflow == null || rrDataflow == null) {
				//System.out.println(key);
			} else {
				displayCompReducerDataflow(rrDataflow, erDataflow, rWriter);
			}
			
		}
		
	}

	private void readFinishedDataflow(String realDataflowDir) {

		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(
					realDataflowDir + "realDataMapper.txt")));
			String[] title = reader.readLine().split("\t");
			String line;

			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				SplitMapperDataflow mDataflow = new SplitMapperDataflow(title, values);
				
				int split = mDataflow.getSplit();
				int xmx = mDataflow.getXmx();
				int xms = mDataflow.getXms();
				int ismb = mDataflow.getIsmb();
				int RN = mDataflow.getRN();

				String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
				String key = split + "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
				mDfMap.put(key, mDataflow);

			}

			reader.close();

			reader = new BufferedReader(new FileReader(new File(realDataflowDir
					+ "realDataReducer.txt")));
			title = reader.readLine().split("\t");

			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				SplitReducerDataflow rDataflow = new SplitReducerDataflow(title, values);
				int split = rDataflow.getSplit();
				int xmx = rDataflow.getXmx();
				int xms = rDataflow.getXms();
				int ismb = rDataflow.getIsmb();
				int RN = rDataflow.getRN();

				String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
				String key = split + "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
				rDfMap.put(key, rDataflow);

			}

			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void displayCompMapperTitle(PrintWriter mWriter) {
		mWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "mInMBdf" + "\t" + "mInRecdf" + "\t" + "mOutMBdf" + "\t"
				+ "mOutRecdf" + "\t" + "mRecBMdf" + "\t" + "mRawBMdf" + "\t"
				+ "mCompBMdf" + "\t" + "mRecAMdf" + "\t" + "mRawAMdf" + "\t"
				+ "mCompAMdf" + "\t" + "mSegNdf" + "\t" + "xInMBdf" + "\t" + "xInRecdf" + "\t"
				+ "xOutMBdf" + "\t" + "xOutRecdf" + "\t" + "xRecBMdf" + "\t"
				+ "xRawBMdf" + "\t" + "xCompBMdf" + "\t" + "xRecAMdf" + "\t"
				+ "xRawAMdf" + "\t" + "xCompAMdf" + "\t" + "xSegNdf" + "\t" + "eJobId" + "\t" + "rJobId" + "\t"
				
				+ "mInMB" + "\t" + "mInRec" + "\t" + "mOutMB" + "\t"
				+ "mOutRec" + "\t" + "mRecBM" + "\t" + "mRawBM" + "\t"
				+ "mCompBM" + "\t" + "mRecAM" + "\t" + "mRawAM" + "\t"
				+ "mCompAM" + "\t" + "mSegN" + "\t" + "xInMB" + "\t" + "xInRec" + "\t"
				+ "xOutMB" + "\t" + "xOutRec" + "\t" + "xRecBM" + "\t"
				+ "xRawBM" + "\t" + "xCompBM" + "\t" + "xRecAM" + "\t"
				+ "xRawAM" + "\t" + "xCompAM" + "\t" + "xSegN"
				);

	}

	private void displayCompReducerTitle(PrintWriter rWriter) {
		rWriter.println("split" + "\t" + "xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN" + "\t" 
				+ "mShCompMBdf" + "\t" + "mShRawMBdf" + "\t" + "mInRecdf" + "\t"  
				+ "mInputMBdf" + "\t" + "mOutRecdf" + "\t" + "mOutMBdf" + "\t"
				+ "xShCompMBdf" + "\t" + "xShRawMBdf" + "\t" + "xInRecdf" + "\t"
				+ "xInputMBdf" + "\t" + "xOutRecdf" + "\t" + "xOutMBdf" + "\t"
				+ "eJobId" + "\t" + "rJobId" + "\t"
				
				+ "mShCompMB" + "\t" + "mShRawMB" + "\t" + "mInRec" + "\t"  
				+ "mInputMB" + "\t" + "mOutRec" + "\t" + "mOutMB" + "\t"
				+ "xShCompMB" + "\t" + "xShRawMB" + "\t" + "xInRec" + "\t"
				+ "xInputMB" + "\t" + "xOutRec" + "\t" + "xOutMB"
				);

	}

	private void displayCompMapperDataflow(SplitMapperDataflow rm,
			SplitMapperDataflow em, PrintWriter mWriter) {

		float mInMBdf = em.getmInMB() - rm.getmInMB();
		float mInRecdf = em.getmInRec() - rm.getmInRec();
		float mOutMBdf = em.getmOutMB() - rm.getmOutMB();
		float mOutRecdf = em.getmOutRec() - rm.getmOutRec();

		float mRecBMdf = em.getmRecBM() - rm.getmRecBM();
		float mRawBMdf = em.getmRawBM() - rm.getmRawBM();
		float mCompBMdf = em.getmCompBM() - rm.getmCompBM();
		float mRecAMdf = em.getmRecAM() - rm.getmRecAM();
		float mRawAMdf = em.getmRawAM() - rm.getmRawAM();
		float mCompAMdf = em.getmCompAM() - rm.getmCompAM();
		int mSegNdf = em.getmSegN() - rm.getmSegN();

		float xInMBdf = em.getxInMB() - rm.getxInMB();
		float xInRecdf = em.getxInRec() - rm.getxInRec();
		float xOutMBdf = em.getxOutMB() - rm.getxOutMB();
		float xOutRecdf = em.getxOutRec() - rm.getxOutRec();

		float xRecBMdf = em.getxRecBM() - rm.getxRecBM();
		float xRawBMdf = em.getxRawBM() - rm.getxRawBM();
		float xCompBMdf = em.getxCompBM() - rm.getxCompBM();
		float xRecAMdf = em.getxRecAM() - rm.getxRecAM();
		float xRawAMdf = em.getxRawAM() - rm.getxRawAM();
		float xCompAMdf = em.getxCompAM() - rm.getxCompAM();
		int xSegNdf = em.getxSegN() - rm.getxSegN();

		float mInMB = rm.getmInMB();
		float mInRec = rm.getmInRec();
		float mOutMB = rm.getmOutMB();
		float mOutRec = rm.getmOutRec();

		float mRecBM = rm.getmRecBM();
		float mRawBM = rm.getmRawBM();
		float mCompBM = rm.getmCompBM();
		float mRecAM = rm.getmRecAM();
		float mRawAM = rm.getmRawAM();
		float mCompAM = rm.getmCompAM();
		int mSegN = rm.getmSegN();

		float xInMB = rm.getxInMB();
		float xInRec = rm.getxInRec();
		float xOutMB = rm.getxOutMB();
		float xOutRec = rm.getxOutRec();

		float xRecBM = rm.getxRecBM();
		float xRawBM = rm.getxRawBM();
		float xCompBM = rm.getxCompBM();
		float xRecAM = rm.getxRecAM();
		float xRawAM = rm.getxRawAM();
		float xCompAM = rm.getxCompAM();
		int xSegN = rm.getxSegN();
		
		int split = rm.getSplit();
		int xmx = rm.getXmx();
		int xms = rm.getXms();
		int ismb = rm.getIsmb();
		int RN = rm.getRN();
		/*
		 * Scanner scanner = new Scanner(key).useDelimiter("[^0-9]+");;;
		 * if(key.contains("Xms")) { xmx = scanner.nextInt(); xms =
		 * scanner.nextInt(); ismb = scanner.nextInt(); RN = scanner.nextInt();
		 * }
		 * 
		 * else { xms = 0; xmx = scanner.nextInt(); ismb = scanner.nextInt(); RN
		 * = scanner.nextInt(); }
		 */

		String f1 = "%1$-2.1f";
		mWriter.println(split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, mInMBdf) + "\t" + String.format(f1, mInRecdf)
				+ "\t" + String.format(f1, mOutMBdf) + "\t"
				+ String.format(f1, mOutRecdf) + "\t" + String.format(f1, mRecBMdf)
				+ "\t" + String.format(f1, mRawBMdf) + "\t"
				+ String.format(f1, mCompBMdf) + "\t" + String.format(f1, mRecAMdf)
				+ "\t" + String.format(f1, mRawAMdf) + "\t"
				+ String.format(f1, mCompAMdf) + "\t" + mSegNdf + "\t"
				+ String.format(f1, xInMBdf) + "\t" + String.format(f1, xInRecdf)
				+ "\t" + String.format(f1, xOutMBdf) + "\t"
				+ String.format(f1, xOutRecdf) + "\t" + String.format(f1, xRecBMdf)
				+ "\t" + String.format(f1, xRawBMdf) + "\t"
				+ String.format(f1, xCompBMdf) + "\t" + String.format(f1, xRecAMdf)
				+ "\t" + String.format(f1, xRawAMdf) + "\t"
				+ String.format(f1, xCompAMdf) + "\t" + xSegNdf + "\t"
				
				+ em.getJobId() + "\t" + rm.getJobId() + "\t"
				
				+ String.format(f1, mInMB) + "\t" + String.format(f1, mInRec)
				+ "\t" + String.format(f1, mOutMB) + "\t"
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mRecBM)
				+ "\t" + String.format(f1, mRawBM) + "\t"
				+ String.format(f1, mCompBM) + "\t" + String.format(f1, mRecAM)
				+ "\t" + String.format(f1, mRawAM) + "\t"
				+ String.format(f1, mCompAM) + "\t" + mSegN + "\t"
				+ String.format(f1, xInMB) + "\t" + String.format(f1, xInRec)
				+ "\t" + String.format(f1, xOutMB) + "\t"
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xRecBM)
				+ "\t" + String.format(f1, xRawBM) + "\t"
				+ String.format(f1, xCompBM) + "\t" + String.format(f1, xRecAM)
				+ "\t" + String.format(f1, xRawAM) + "\t"
				+ String.format(f1, xCompAM) + "\t" + xSegN + "\t"
				);
	}

	private void displayCompReducerDataflow(SplitReducerDataflow rr,
			SplitReducerDataflow er, PrintWriter rWriter) {
		float mShCompMBdf = er.getmShCompMB() - rr.getmShCompMB();
		float mShRawMBdf = er.getmShRawMB() - rr.getmShRawMB();
		float mInRecdf = er.getmInRec() - rr.getmInRec();
		float mInputMBdf = er.getmInputMB() - rr.getmInputMB();
		/*
		if(mInputMB < -1000) {
			System.out.println(rr + "\t" + rr.getXmx() + "\t" + rr.getXms() + "\t" + rr.getIsmb() + "\t" + rr.getRN() + "\t" + rr.getJobId());
			
			System.out.println(er + "\t" + er.getJobId());
		}
		*/
		float mOutRecdf = er.getmOutRec() - rr.getmOutRec();
		float mOutMBdf = er.getmOutMB() - rr.getmOutMB();

		float xShCompMBdf = er.getxShCompMB() - rr.getxShCompMB();
		float xShRawMBdf = er.getxShRawMB() - rr.getxShRawMB();
		float xInRecdf = er.getxInRec() - rr.getxInRec();
		float xInputMBdf = er.getxInputMB() - rr.getxInputMB();
		float xOutRecdf = er.getxOutRec() - rr.getxOutRec();
		float xOutMBdf = er.getxOutMB() - rr.getxOutMB();

		int split = rr.getSplit();
		int xmx = rr.getXmx();
		int xms = rr.getXms();
		int ismb = rr.getIsmb();
		int RN = rr.getRN();

		float mShCompMB = rr.getmShCompMB();
		float mShRawMB = rr.getmShRawMB();
		float mInRec = rr.getmInRec();
		float mInputMB = rr.getmInputMB();
		/*
		if(mInputMB < -1000) {
			System.out.println(rr + "\t" + rr.getXmx() + "\t" + rr.getXms() + "\t" + rr.getIsmb() + "\t" + rr.getRN() + "\t" + rr.getJobId());
			
			System.out.println(er + "\t" + er.getJobId());
		}
		*/
		float mOutRec = rr.getmOutRec();
		float mOutMB = rr.getmOutMB();

		float xShCompMB = rr.getxShCompMB();
		float xShRawMB = rr.getxShRawMB();
		float xInRec = rr.getxInRec();
		float xInputMB = rr.getxInputMB();
		float xOutRec = rr.getxOutRec();
		float xOutMB = rr.getxOutMB();
		
		/*
		 * Scanner scanner = new Scanner(key).useDelimiter("[^0-9]+");;;
		 * if(key.contains("Xms")) { xmx = scanner.nextInt(); xms =
		 * scanner.nextInt(); ismb = scanner.nextInt(); RN = scanner.nextInt();
		 * }
		 * 
		 * else { xms = 0; xmx = scanner.nextInt(); ismb = scanner.nextInt(); RN
		 * = scanner.nextInt(); }
		 */
		String f1 = "%1$-2.1f";
		rWriter.println(split + "\t" + xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, mShCompMBdf) + "\t"
				+ String.format(f1, mShRawMBdf) + "\t"
				+ String.format(f1, mInRecdf) + "\t"
				+ String.format(f1, mInputMBdf) + "\t"
				+ String.format(f1, mOutRecdf) + "\t" + String.format(f1, mOutMBdf)
				+ "\t" + String.format(f1, xShCompMBdf) + "\t"
				+ String.format(f1, xShRawMBdf) + "\t"
				+ String.format(f1, xInRecdf) + "\t"
				+ String.format(f1, xInputMBdf) + "\t"
				+ String.format(f1, xOutRecdf) + "\t" + String.format(f1, xOutMBdf)
				
				+ "\t" + er.getJobId() + "\t" + rr.getJobId() + "\t"
				
				+ String.format(f1, mShCompMB) + "\t"
				+ String.format(f1, mShRawMB) + "\t"
				+ String.format(f1, mInRec) + "\t"
				+ String.format(f1, mInputMB) + "\t"
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mOutMB)
				+ "\t" + String.format(f1, xShCompMB) + "\t"
				+ String.format(f1, xShRawMB) + "\t"
				+ String.format(f1, xInRec) + "\t"
				+ String.format(f1, xInputMB) + "\t"
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xOutMB));
	}
	
	public static void main(String[] args) {
		String jobName = "BuildCompIndex-m36-r18-256MB";
		String baseDir = "/home/xulijie/MR-MEM/NewExperiments2/";
		int splitMB[] = {64, 128, 256};
		
		SplitDataflowComparator dataComp = new SplitDataflowComparator(jobName, baseDir);
		dataComp.compareDataflow();
	}

}
