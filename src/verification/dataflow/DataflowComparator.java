package verification.dataflow;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


public class DataflowComparator {
	private Map<String, MapperDataflow> mDfMap = new HashMap<String, MapperDataflow>();
	private Map<String, ReducerDataflow> rDfMap = new HashMap<String, ReducerDataflow>();

	private String jobName;
	private String baseDir;
	
	public DataflowComparator(String jobName, String baseDir) {
		this.jobName = jobName;
		this.baseDir = baseDir;
	}

	public void compareDataflow() {
		String realDataflowDir = baseDir + jobName + "/RealDataflow/";
		String estimatedJvmCostDir = baseDir + jobName + "/estimatedJvmCost/";
		String compDataflow = baseDir + jobName + "/compDataflow/";

		readFinishedDataflow(realDataflowDir);
		compDataflow(estimatedJvmCostDir, compDataflow);

		System.out.println("[" + jobName + "] dataflow comparison finished");
	}
	
	
	private void compDataflow(String estimatedJvmCostDir, String compDataflow) {
		File eDir = new File(estimatedJvmCostDir);
		File compJobIdDir = new File(compDataflow);
		if (!compJobIdDir.exists())
			compJobIdDir.mkdirs();

		try {
			PrintWriter mWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(compJobIdDir, "compMappers.txt"))));
			PrintWriter rWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(compJobIdDir, "compReducers.txt"))));
			
			displayCompMapperTitle(mWriter);
			displayCompReducerTitle(rWriter);

			for (File jobIdFile : eDir.listFiles()) {
				File eDataflowFile = new File(jobIdFile, "eDataflow");

				for (File eDF : eDataflowFile.listFiles()) {

					// boolean b = eDF.renameTo(new
					// File(eDF.getAbsolutePath().replaceAll(" ", "")));
					// if(b == false)
					// System.out.println("Failed to rename " + eDF.getName());

					String key = eDF.getName();
					MapperDataflow emDataflow = new MapperDataflow(eDF.getAbsolutePath() + File.separator + "eDataMappers.txt");
					ReducerDataflow erDataflow = new ReducerDataflow(eDF.getAbsolutePath() + File.separator + "eDataReducers.txt");
					emDataflow.setJobId(jobIdFile.getName());
					erDataflow.setJobId(jobIdFile.getName());
					
					MapperDataflow rmDataflow = mDfMap.get(key);
					ReducerDataflow rrDataflow = rDfMap.get(key);

					if (rmDataflow == null || rrDataflow == null) {
						//System.out.println(key);
					} else {
						displayCompMapperDataflow(rmDataflow, emDataflow, mWriter);
						displayCompReducerDataflow(rrDataflow, erDataflow, rWriter);
					}

				}

				
			}

			mWriter.close();
			rWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				MapperDataflow mDataflow = new MapperDataflow(title, values);
				int xmx = mDataflow.getXmx();
				int xms = mDataflow.getXms();
				int ismb = mDataflow.getIsmb();
				int RN = mDataflow.getRN();

				String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
				String key = "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
				mDfMap.put(key, mDataflow);

			}

			reader.close();

			reader = new BufferedReader(new FileReader(new File(realDataflowDir
					+ "realDataReducer.txt")));
			title = reader.readLine().split("\t");

			while ((line = reader.readLine()) != null) {
				String[] values = line.split("\t");
				ReducerDataflow rDataflow = new ReducerDataflow(title, values);
				int xmx = rDataflow.getXmx();
				int xms = rDataflow.getXms();
				int ismb = rDataflow.getIsmb();
				int RN = rDataflow.getRN();

				String xmsStr = xms == 0 ? "" : "-Xms" + xms + "m";
				String key = "-Xmx" + xmx + "m" + xmsStr + "-ismb" + ismb + "-RN" + RN;
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
		mWriter.println("xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN"
				+ "\t" + "mInMB" + "\t" + "mInRec" + "\t" + "mOutMB" + "\t"
				+ "mOutRec" + "\t" + "mRecBM" + "\t" + "mRawBM" + "\t"
				+ "mCompBM" + "\t" + "mRecAM" + "\t" + "mRawAM" + "\t"
				+ "mCompAM" + "\t" + "mSegN" + "\t" + "xInMB" + "\t" + "xInRec"
				+ "\t" + "xOutMB" + "\t" + "xOutRec" + "\t" + "xRecBM" + "\t"
				+ "xRawBM" + "\t" + "xCompBM" + "\t" + "xRecAM" + "\t"
				+ "xRawAM" + "\t" + "xCompAM" + "\t" + "xSegN" + "\t" + "eJobId" + "\t" + "rJobId");

	}

	private void displayCompReducerTitle(PrintWriter rWriter) {
		rWriter.println("xmx" + "\t" + "xms" + "\t" + "ismb" + "\t" + "RN"
				+ "\t" + "mShCompMB" + "\t" + "mShRawMB" + "\t" + "mInRec"
				+ "\t" + "mInputMB" + "\t" + "mOutRec" + "\t" + "mOutMB" + "\t"
				+ "xShCompMB" + "\t" + "xShRawMB" + "\t" + "xInRec" + "\t"
				+ "xInputMB" + "\t" + "xOutRec" + "\t" + "xOutMB" + "\t"
				+ "eJobId" + "\t" + "rJobId");

	}

	private void displayCompMapperDataflow(MapperDataflow rm,
			MapperDataflow em, PrintWriter mWriter) {

		float mInMB = em.getmInMB() - rm.getmInMB();
		float mInRec = em.getmInRec() - rm.getmInRec();
		float mOutMB = em.getmOutMB() - rm.getmOutMB();
		float mOutRec = em.getmOutRec() - rm.getmOutRec();

		float mRecBM = em.getmRecBM() - rm.getmRecBM();
		float mRawBM = em.getmRawBM() - rm.getmRawBM();
		float mCompBM = em.getmCompBM() - rm.getmCompBM();
		float mRecAM = em.getmRecAM() - rm.getmRecAM();
		float mRawAM = em.getmRawAM() - rm.getmRawAM();
		float mCompAM = em.getmCompAM() - rm.getmCompAM();
		int mSegN = em.getmSegN() - rm.getmSegN();

		float xInMB = em.getxInMB() - rm.getxInMB();
		float xInRec = em.getxInRec() - rm.getxInRec();
		float xOutMB = em.getxOutMB() - rm.getxOutMB();
		float xOutRec = em.getxOutRec() - rm.getxOutRec();

		float xRecBM = em.getxRecBM() - rm.getxRecBM();
		float xRawBM = em.getxRawBM() - rm.getxRawBM();
		float xCompBM = em.getxCompBM() - rm.getxCompBM();
		float xRecAM = em.getxRecAM() - rm.getxRecAM();
		float xRawAM = em.getxRawAM() - rm.getxRawAM();
		float xCompAM = em.getxCompAM() - rm.getxCompAM();
		int xSegN = em.getxSegN() - rm.getxSegN();

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
		mWriter.println(xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
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
				+ em.getJobId() + "\t" + rm.getJobId());
	}

	private void displayCompReducerDataflow(ReducerDataflow rr,
			ReducerDataflow er, PrintWriter rWriter) {
		float mShCompMB = er.getmShCompMB() - rr.getmShCompMB();
		float mShRawMB = er.getmShRawMB() - rr.getmShRawMB();
		float mInRec = er.getmInRec() - rr.getmInRec();
		float mInputMB = er.getmInputMB() - rr.getmInputMB();
		/*
		if(mInputMB < -1000) {
			System.out.println(rr + "\t" + rr.getXmx() + "\t" + rr.getXms() + "\t" + rr.getIsmb() + "\t" + rr.getRN() + "\t" + rr.getJobId());
			
			System.out.println(er + "\t" + er.getJobId());
		}
		*/
		float mOutRec = er.getmOutRec() - rr.getmOutRec();
		float mOutMB = er.getmOutMB() - rr.getmOutMB();

		float xShCompMB = er.getxShCompMB() - rr.getxShCompMB();
		float xShRawMB = er.getxShRawMB() - rr.getxShRawMB();
		float xInRec = er.getxInRec() - rr.getxInRec();
		float xInputMB = er.getxInputMB() - rr.getxInputMB();
		float xOutRec = er.getxOutRec() - rr.getxOutRec();
		float xOutMB = er.getxOutMB() - rr.getxOutMB();

		int xmx = rr.getXmx();
		int xms = rr.getXms();
		int ismb = rr.getIsmb();
		int RN = rr.getRN();

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
		rWriter.println(xmx + "\t" + xms + "\t" + ismb + "\t" + RN + "\t"
				+ String.format(f1, mShCompMB) + "\t"
				+ String.format(f1, mShRawMB) + "\t"
				+ String.format(f1, mInRec) + "\t"
				+ String.format(f1, mInputMB) + "\t"
				+ String.format(f1, mOutRec) + "\t" + String.format(f1, mOutMB)
				+ "\t" + String.format(f1, xShCompMB) + "\t"
				+ String.format(f1, xShRawMB) + "\t"
				+ String.format(f1, xInRec) + "\t"
				+ String.format(f1, xInputMB) + "\t"
				+ String.format(f1, xOutRec) + "\t" + String.format(f1, xOutMB)
				+ "\t" + er.getJobId() + "\t" + rr.getJobId());
	}
	
	public static void main(String[] args) {
		String jobName = "BuildCompIndex-m36-r18-256MB";
		String baseDir = "/home/xulijie/MR-MEM/NewExperiments2/";
	
		DataflowComparator dataComp = new DataflowComparator(jobName, baseDir);
		dataComp.compareDataflow();
	}

}
