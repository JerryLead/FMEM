package verification.split.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class SplitMapperDataDiff {

	private List<Float> mInMBdf = new ArrayList<Float>();
	private List<Float> mInRecdf = new ArrayList<Float>();
	private List<Float> mOutMBdf = new ArrayList<Float>();
	private List<Float> mOutRecdf = new ArrayList<Float>();
	private List<Float> mRecBMdf = new ArrayList<Float>();
	private List<Float> mRawBMdf = new ArrayList<Float>();
	private List<Float> mCompBMdf = new ArrayList<Float>();
	private List<Float> mRecAMdf = new ArrayList<Float>();
	private List<Float> mRawAMdf = new ArrayList<Float>();
	private List<Float> mCompAMdf = new ArrayList<Float>();
	private List<Integer> mSegNdf = new ArrayList<Integer>();
	private List<Float> xInMBdf = new ArrayList<Float>();
	private List<Float> xInRecdf = new ArrayList<Float>();
	private List<Float> xOutMBdf = new ArrayList<Float>();
	private List<Float> xOutRecdf = new ArrayList<Float>();
	
	private List<Float> xRecBMdf = new ArrayList<Float>();
	private List<Float> xRawBMdf = new ArrayList<Float>();
	private List<Float> xCompBMdf = new ArrayList<Float>();
	private List<Float> xRecAMdf = new ArrayList<Float>();
	private List<Float> xRawAMdf = new ArrayList<Float>();
	private List<Float> xCompAMdf = new ArrayList<Float>();
	private List<Integer> xSegNdf = new ArrayList<Integer>();
	
	private List<String> eJobId = new ArrayList<String>();
	private List<String> rJobId = new ArrayList<String>();
	
	private List<Float> mInMB = new ArrayList<Float>();
	private List<Float> mInRec = new ArrayList<Float>();
	private List<Float> mOutMB = new ArrayList<Float>();
	private List<Float> mOutRec = new ArrayList<Float>();
	private List<Float> mRecBM = new ArrayList<Float>();
	private List<Float> mRawBM = new ArrayList<Float>();
	private List<Float> mCompBM = new ArrayList<Float>();
	private List<Float> mRecAM = new ArrayList<Float>();
	private List<Float> mRawAM = new ArrayList<Float>();
	private List<Float> mCompAM = new ArrayList<Float>();
	private List<Integer> mSegN = new ArrayList<Integer>();
	private List<Float> xInMB = new ArrayList<Float>();
	private List<Float> xInRec = new ArrayList<Float>();
	private List<Float> xOutMB = new ArrayList<Float>();
	private List<Float> xOutRec = new ArrayList<Float>();
	
	private List<Float> xRecBM = new ArrayList<Float>();
	private List<Float> xRawBM = new ArrayList<Float>();
	private List<Float> xCompBM = new ArrayList<Float>();
	private List<Float> xRecAM = new ArrayList<Float>();
	private List<Float> xRawAM = new ArrayList<Float>();
	private List<Float> xCompAM = new ArrayList<Float>();
	private List<Integer> xSegN = new ArrayList<Integer>();
	
	public SplitMapperDataDiff(File diffMapperFile) {
		readFile(diffMapperFile);
	}

	public List<Pair> sortList(String title) {
				
		List<Pair> result = null;
		
		if(title.equals("mInMB"))
			result = Pair.generatePairs(mInMB, mInMBdf);
		else if(title.equals("mInRec"))
			result = Pair.generatePairs(mInRec, mInRecdf);
		else if(title.equals("mOutMB"))
			result = Pair.generatePairs(mOutMB, mOutMBdf);
		else if(title.equals("mOutRec"))
			result = Pair.generatePairs(mOutRec, mOutRecdf);
		else if(title.equals("mRecBM"))
			result = Pair.generatePairs(mRecBM, mRecBMdf);
		else if(title.equals("mRawBM"))
			result = Pair.generatePairs(mRawBM, mRawBMdf);
		else if(title.equals("mCompBM"))
			result = Pair.generatePairs(mCompBM, mCompBMdf);
		else if(title.equals("mRecAM"))
			result = Pair.generatePairs(mRecAM, mRecAMdf);
		else if(title.equals("mRawAM"))
			result = Pair.generatePairs(mRawAM, mRawAMdf);
		else if(title.equals("mCompAM"))
			result = Pair.generatePairs(mCompAM, mCompAMdf);
		else if(title.equals("xInMB"))
			result = Pair.generatePairs(xInMB, xInMBdf);
		else if(title.equals("xInRec"))
			result = Pair.generatePairs(xInRec, xInRecdf);
		else if(title.equals("xOutMB"))
			result = Pair.generatePairs(xOutMB, xOutMBdf);
		else if(title.equals("xOutRec"))
			result = Pair.generatePairs(xOutRec, xOutRecdf);
		else if(title.equals("xRecBM"))
			result = Pair.generatePairs(xRecBM, xRecBMdf);
		else if(title.equals("xRawBM"))
			result = Pair.generatePairs(xRawBM, xRawBMdf);
		else if(title.equals("xCompBM"))
			result = Pair.generatePairs(xCompBM, xCompBMdf);
		else if(title.equals("xRecAM"))
			result = Pair.generatePairs(xRecAM, xRecAMdf);
		else if(title.equals("xRawAM")) 
			result = Pair.generatePairs(xRawAM, xRawAMdf);
			
		else if(title.equals("xCompAM"))
			result = Pair.generatePairs(xCompAM, xCompAMdf);
		
		
		Collections.sort(result);
		
		return result;
		
	}

	
	
	private void readFile(File diffMapperFile) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(diffMapperFile));
			String[] title = reader.readLine().split("\t");
			String line;
			
			while((line = reader.readLine()) != null) {
				String[] value = line.split("\t");
				for(int i = 0; i < title.length; i++) 
					addValue(title[i], value[i]);
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
	
	private void addValue(String title, String value) {
		if(title.equalsIgnoreCase("eJobId"))
			eJobId.add(value);
		else if(title.equalsIgnoreCase("rJobId"))
			rJobId.add(value);
		else if(title.equals("mSegN"))
			mSegN.add(Integer.parseInt(value));
		else if(title.equals("xSegN"))
			xSegN.add(Integer.parseInt(value));
		else {
			float v = Float.parseFloat(value);
			//v = Math.abs(v); // we want to get the absolute difference
			if(title.equals("mInMBdf"))
				mInMBdf.add(v);
			else if(title.equals("mInRecdf"))
				mInRecdf.add(v);
			else if(title.equals("mOutMBdf"))
				mOutMBdf.add(v);
			else if(title.equals("mOutRecdf"))
				mOutRecdf.add(v);
			else if(title.equals("mRecBMdf"))
				mRecBMdf.add(v);
			else if(title.equals("mRawBMdf"))
				mRawBMdf.add(v);
			else if(title.equals("mCompBMdf"))
				mCompBMdf.add(v);
			else if(title.equals("mRecAMdf"))
				mRecAMdf.add(v);
			else if(title.equals("mRawAMdf")) 
				mRawAMdf.add(v);
				
				
			else if(title.equals("mCompAMdf"))
				mCompAMdf.add(v);
			else if(title.equals("xInMBdf"))
				xInMBdf.add(v);
			else if(title.equals("xInRecdf"))
				xInRecdf.add(v);
			else if(title.equals("xOutMBdf"))
				xOutMBdf.add(v);
			else if(title.equals("xOutRecdf"))
				xOutRecdf.add(v);
			else if(title.equals("xRecBMdf"))
				xRecBMdf.add(v);
			else if(title.equals("xRawBMdf"))
				xRawBMdf.add(v);
			else if(title.equals("xCompBMdf"))
				xCompBMdf.add(v);
			else if(title.equals("xRecAMdf"))
				xRecAMdf.add(v);
			else if(title.equals("xRawAMdf"))
				xRawAMdf.add(v);
			else if(title.equals("xCompAMdf"))
				xCompAMdf.add(v);
			
			else if(title.equals("mInMB"))
				mInMB.add(v);
			else if(title.equals("mInRec"))
				mInRec.add(v);
			else if(title.equals("mOutMB"))
				mOutMB.add(v);
			else if(title.equals("mOutRec"))
				mOutRec.add(v);
			else if(title.equals("mRecBM"))
				mRecBM.add(v);
			else if(title.equals("mRawBM"))
				mRawBM.add(v);
			else if(title.equals("mCompBM"))
				mCompBM.add(v);
			else if(title.equals("mRecAM"))
				mRecAM.add(v);
			else if(title.equals("mRawAM"))
				mRawAM.add(v);
			else if(title.equals("mCompAM"))
				mCompAM.add(v);
			else if(title.equals("xInMB"))
				xInMB.add(v);
			else if(title.equals("xInRec"))
				xInRec.add(v);
			else if(title.equals("xOutMB"))
				xOutMB.add(v);
			else if(title.equals("xOutRec"))
				xOutRec.add(v);
			else if(title.equals("xRecBM"))
				xRecBM.add(v);
			else if(title.equals("xRawBM"))
				xRawBM.add(v);
			else if(title.equals("xCompBM"))
				xCompBM.add(v);
			else if(title.equals("xRecAM"))
				xRecAM.add(v);
			else if(title.equals("xRawAM"))
				xRawAM.add(v);
			else if(title.equals("xCompAM"))
				xCompAM.add(v);

		}
	}

	
	
}

