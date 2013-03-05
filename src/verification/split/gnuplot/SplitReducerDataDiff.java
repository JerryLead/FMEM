package verification.split.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SplitReducerDataDiff {

	private List<Float> mShCompMBdf = new ArrayList<Float>();
	private List<Float> mShRawMBdf = new ArrayList<Float>();
	private List<Float> mInRecdf = new ArrayList<Float>();
	private List<Float> mInputMBdf = new ArrayList<Float>();
	private List<Float> mOutRecdf	= new ArrayList<Float>();
	private List<Float> mOutMBdf = new ArrayList<Float>();
	private List<Float> xShCompMBdf = new ArrayList<Float>();
	private List<Float> xShRawMBdf = new ArrayList<Float>();
	private List<Float> xInRecdf = new ArrayList<Float>();
	private List<Float> xInputMBdf = new ArrayList<Float>();
	private List<Float> xOutRecdf = new ArrayList<Float>();
	private List<Float> xOutMBdf = new ArrayList<Float>();
	private List<String> eJobId = new ArrayList<String>();
	private List<String> rJobId = new ArrayList<String>();
	
	private List<Float> mShCompMB = new ArrayList<Float>();
	private List<Float> mShRawMB = new ArrayList<Float>();
	private List<Float> mInRec = new ArrayList<Float>();
	private List<Float> mInputMB = new ArrayList<Float>();
	private List<Float> mOutRec	= new ArrayList<Float>();
	private List<Float> mOutMB = new ArrayList<Float>();
	private List<Float> xShCompMB = new ArrayList<Float>();
	private List<Float> xShRawMB = new ArrayList<Float>();
	private List<Float> xInRec = new ArrayList<Float>();
	private List<Float> xInputMB = new ArrayList<Float>();
	private List<Float> xOutRec = new ArrayList<Float>();
	private List<Float> xOutMB = new ArrayList<Float>();
	
	public SplitReducerDataDiff(File diffReducerFile) {
		readFile(diffReducerFile);
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
	
		else {
			float v = Float.parseFloat(value);
			//v = Math.abs(v); // we want the absolute value
			if(title.equals("mShCompMB"))
				mShCompMB.add(v);
			else if(title.equals("mShRawMB"))
				mShRawMB.add(v);
			else if(title.equals("mInRec"))
				mInRec.add(v);
			else if(title.equals("mInputMB"))
				mInputMB.add(v);
			else if(title.equals("mOutRec"))
				mOutRec.add(v);
			else if(title.equals("mOutMB"))
				mOutMB.add(v);
			else if(title.equals("xShCompMB"))
				xShCompMB.add(v);
			else if(title.equals("xShRawMB"))
				xShRawMB.add(v);
			else if(title.equals("xInRec"))
				xInRec.add(v);
			else if(title.equals("xInputMB"))
				xInputMB.add(v);
			else if(title.equals("xOutRec"))
				xOutRec.add(v);
			else if(title.equals("xOutMB"))
				xOutMB.add(v);
			
			else if(title.equals("mShCompMBdf"))
				mShCompMBdf.add(v);
			else if(title.equals("mShRawMBdf"))
				mShRawMBdf.add(v);
			else if(title.equals("mInRecdf"))
				mInRecdf.add(v);
			else if(title.equals("mInputMBdf"))
				mInputMBdf.add(v);
			else if(title.equals("mOutRecdf"))
				mOutRecdf.add(v);
			else if(title.equals("mOutMBdf"))
				mOutMBdf.add(v);
			else if(title.equals("xShCompMBdf"))
				xShCompMBdf.add(v);
			else if(title.equals("xShRawMBdf"))
				xShRawMBdf.add(v);
			else if(title.equals("xInRecdf"))
				xInRecdf.add(v);
			else if(title.equals("xInputMBdf"))
				xInputMBdf.add(v);
			else if(title.equals("xOutRecdf"))
				xOutRecdf.add(v);
			else if(title.equals("xOutMBdf"))
				xOutMBdf.add(v);
		}
	}
	
	public List<Pair> sortList(String title) {
		
		List<Pair> result = null;
		
		if(title.equals("mShCompMB"))
			result = Pair.generatePairs(mShCompMB, mShCompMBdf);
		else if(title.equals("mShRawMB"))
			result = Pair.generatePairs(mShRawMB, mShRawMBdf);
		else if(title.equals("mInRec"))
			result = Pair.generatePairs(mInRec, mInRecdf);
		else if(title.equals("mInputMB"))
			result = Pair.generatePairs(mInputMB, mInputMBdf);
		else if(title.equals("mOutRec"))
			result = Pair.generatePairs(mOutRec, mOutRecdf);
		else if(title.equals("mOutMB"))
			result = Pair.generatePairs(mOutMB, mOutMBdf);
		else if(title.equals("xShCompMB"))
			result = Pair.generatePairs(xShCompMB, xShCompMBdf);
		else if(title.equals("xShRawMB"))
			result = Pair.generatePairs(xShRawMB, xShRawMBdf);
		else if(title.equals("xInRec"))
			result = Pair.generatePairs(xInRec, xInRecdf);
		else if(title.equals("xInputMB"))
			result = Pair.generatePairs(xInputMB, xInputMBdf);
		else if(title.equals("xOutRec"))
			result = Pair.generatePairs(xOutRec, xOutRecdf);
		else if(title.equals("xOutMB"))
			result = Pair.generatePairs(xOutMB, xOutMBdf);
		
		Collections.sort(result);
		
		return result;
	}

	
	
}
