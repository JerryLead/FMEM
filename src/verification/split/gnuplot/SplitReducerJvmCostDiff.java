package verification.split.gnuplot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SplitReducerJvmCostDiff {
																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																		
																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																										
	private List<Float> mOUdf = new ArrayList<Float>();
	private List<Float> xOUdf = new ArrayList<Float>();
	private List<Float> mNGUdf = new ArrayList<Float>();
	private List<Float> xNGUdf = new ArrayList<Float>();
	private List<Float> xHeapUdf = new ArrayList<Float>();
	private List<Float> RSSdf = new ArrayList<Float>();
	
	private List<Float> mOUrt = new ArrayList<Float>();
	private List<Float> xOUrt = new ArrayList<Float>();
	private List<Float> mNGUrt = new ArrayList<Float>();
	private List<Float> xNGUrt = new ArrayList<Float>();
	private List<Float> xHeapUrt = new ArrayList<Float>();
	private List<Float> RSSrt = new ArrayList<Float>();
	
	private List<Float> rmOU = new ArrayList<Float>();
	private List<Float> rxOU = new ArrayList<Float>();
	private List<Float> exOU = new ArrayList<Float>();
	private List<Float> rmNGU = new ArrayList<Float>();
	private List<Float> rxNGU = new ArrayList<Float>();
	private List<Float> exNGU = new ArrayList<Float>();
	private List<Float> rxHeapU = new ArrayList<Float>();
	private List<Float> rxRSS = new ArrayList<Float>();
	private List<Float> exHeapU = new ArrayList<Float>();
	
	
	public SplitReducerJvmCostDiff(File diffReducerFile) {
		readFile(diffReducerFile);
	}

	public List<Pair> sortList(String title) {
		
		List<Pair> result = null;
	
		if(title.equals("mOU"))
			result = Pair.generatePairs(rmOU, mOUdf);
		else if(title.equals("xOU"))
			result = Pair.generatePairs(rxOU, xOUdf);
		else if(title.equals("mNGU"))
			result = Pair.generatePairs(rmNGU, mNGUdf);
		else if(title.equals("xNGU"))
			result = Pair.generatePairs(rxNGU, xNGUdf);
		else if(title.equals("xHeapU"))
			result = Pair.generatePairs(rxHeapU, xHeapUdf);
		else if(title.equals("RSS"))
			result = Pair.generatePairs(rxRSS, RSSdf);
		
		
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
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void addValue(String title, String value) {
		if(title.equalsIgnoreCase("eJobId") || title.equalsIgnoreCase("rJobId"))
			return;
		float v = Float.parseFloat(value);
		if(title.equals("mOUdf"))
			mOUdf.add(v);
		else if(title.equals("xOUdf"))
			xOUdf.add(v);
		else if(title.equals("mNGUdf"))
			mNGUdf.add(v);
		else if(title.equals("xNGUdf"))
			xNGUdf.add(v);
		else if(title.equals("xHeapUdf"))
			xHeapUdf.add(v);
		else if(title.equals("RSSdf"))
			RSSdf.add(v);
		
		else if(title.equals("mOUrt"))
			mOUrt.add(v);
		else if(title.equals("xOUrt"))
			xOUrt.add(v);
		else if(title.equals("mNGUrt"))
			mNGUrt.add(v);
		else if(title.equals("xNGUrt"))
			xNGUrt.add(v);
		else if(title.equals("xHeapUrt"))
			xHeapUrt.add(v);
		else if(title.equals("RSSrt"))
			RSSrt.add(v);
		
		else if(title.equals("rmOU"))
			rmOU.add(v);
		else if(title.equals("rxOU"))
			rxOU.add(v);
		else if(title.equals("exOU"))
			exOU.add(v);
		else if(title.equals("rmNGU"))
			rmNGU.add(v);
		else if(title.equals("rxNGU"))
			rxNGU.add(v);
		else if(title.equals("exNGU"))
			exNGU.add(v);
		else if(title.equals("rxHeapU"))
			rxHeapU.add(v);
		else if(title.equals("rxRSS"))
			rxRSS.add(v);
		else if(title.equals("exHeapU"))
			exHeapU.add(v);
	}


	
}
