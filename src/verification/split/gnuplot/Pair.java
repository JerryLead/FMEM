package verification.split.gnuplot;

import java.util.ArrayList;
import java.util.List;


public class Pair implements Comparable<Pair> {
	float real;
	float estimated;
	
	public Pair(float real, float df) {
		this.real = real;
		this.estimated = real + df;
	}

	@Override
	public int compareTo(Pair o) {
		if(real < o.getReal())
			return -1;
		else if(real > o.getReal())
			return 1;
		
		return 0;
	}

	public float getReal() {
		return real;
	}

	public float getEstimated() {
		return estimated;
	}

	public static List<Pair> generatePairs(List<Float> realList, List<Float> dfList) {
		List<Pair> list = new ArrayList<Pair>();
		float real, df;
		
		for(int i = 0; i < realList.size(); i++) {
			real = realList.get(i);
			df = dfList.get(i);
			list.add(new Pair(real, df));
			
		}
		
		return list;
	}
	
}