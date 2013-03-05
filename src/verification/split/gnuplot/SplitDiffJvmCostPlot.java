package verification.split.gnuplot;

import java.util.List;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.Style;

public class SplitDiffJvmCostPlot {
	private String jobName;
	private String baseDir;
	
	public SplitDiffJvmCostPlot(String jobName, String baseDir) {
		this.jobName = jobName;
		this.baseDir = baseDir;
	}
	
	public static void main(String[] args) {
		String jobName = "Wiki-m36-r18-256MB";
		String baseDir = "/home/xulijie/MR-MEM/NewExperiments/";
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/compJvmCost/";
		//String absolErrorImageDir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/AbsolErrorImage/";
		
		//String compJvmCostDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/compJvmCost/";
		//String absolErrorImageDir = "/home/xulijie/MR-MEM/NewExperiments/Wiki-m36-r18-256MB/AbsolErrorImage/";
		SplitDiffJvmCostPlot diffJvmPlot = new SplitDiffJvmCostPlot(jobName, baseDir);
		diffJvmPlot.visualize();	
	}

	public void visualize() {
		String compJvmCostDir = baseDir + jobName + "/compJvmCost/";
		String diffImageDir = baseDir + jobName + "/ErrorImage/jvmCostError/";
		
		String diffMapper = "diffMappers.txt";
		String diffReducer = "diffReducers.txt";
		
		
		File diffMapperFile = new File(compJvmCostDir + diffMapper);
		File diffReducerFile = new File(compJvmCostDir + diffReducer);
		
	
		SplitMapperJvmCostDiff mDiff = readMapperArray(diffMapperFile);
		SplitReducerJvmCostDiff rDiff = readReducerArray(diffReducerFile);
		
		plotMapperDiff(mDiff, diffImageDir);
		plotReducerDiff(rDiff, diffImageDir);	
		
		System.out.println("Finish ploting Jvm Cost Difference");
		
	}

	private void plotMapperDiff(SplitMapperJvmCostDiff mDiff, String diffImageDir) {
		List<Pair> rxOUList = mDiff.sortList("rxOU");
		List<Pair> rxNGUList = mDiff.sortList("rxNGU");
		List<Pair> rxRSSList = mDiff.sortList("rxRSS");
		List<Pair> xHeapUList = mDiff.sortList("rxHeapU");
		
		
		File diffImgDirFile = new File(diffImageDir);
		if(!diffImgDirFile.exists())
			diffImgDirFile.mkdirs();
		
		BufferedImage image = plotPNG(rxOUList, "Mapper xOU Real and Estimated", "xOU", "Bytes (MB)");
		outputImage(image, diffImageDir + "Mapper-xOU.png");
		image = plotPNG(rxNGUList, "Mapper xNGU Real and Estimated", "xNGU", "Bytes (MB)");
		outputImage(image, diffImageDir + "Mapper-xNGU.png");
		image = plotPNG(rxRSSList, "Mapper RSS Real and Estimated", "RSS", "Bytes (MB)");
		outputImage(image, diffImageDir + "Mapper-RSS.png");
		image = plotPNG(xHeapUList, "Mapper xHeapU Real and Estimated", "xHeapU", "Bytes (MB)");
		outputImage(image, diffImageDir + "Mapper-xHeapU.png");
	}
	
	private void outputImage(BufferedImage plotPNG, String title) {
		
		try {
			ImageIO.write(plotPNG, "PNG", new File(title));
			plotPNG.flush();  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
	}

	private void plotReducerDiff(SplitReducerJvmCostDiff rDiff, String diffImageDir) {
		List<Pair> xOUdfList = rDiff.sortList("xOU");
		List<Pair> xNGUdfList = rDiff.sortList("xNGU");
		List<Pair> RSSdfList = rDiff.sortList("RSS");
		List<Pair> xHeapUdfList = rDiff.sortList("xHeapU");
		
		String absImgDir = diffImageDir;
		File absImgFile = new File(absImgDir);
		if(!absImgFile.exists())
			absImgFile.mkdirs();
		
		BufferedImage image = plotPNG(xOUdfList, "Reducer xOU Real and Estimated", "xOU", "Bytes (MB)");
		outputImage(image, absImgDir + "Reducer-xOU.png");
		image = plotPNG(xNGUdfList, "Reducer xNGU Real and Estimated", "xNGU", "Bytes (MB)");
		outputImage(image, absImgDir + "Reducer-xNGU.png");
		image = plotPNG(RSSdfList, "Reducer RSS Real and Estimated", "RSS", "Bytes (MB)");
		outputImage(image, absImgDir + "Reducer-RSS.png");
		image = plotPNG(xHeapUdfList, "Reducer xHeapU Real and Estimated", "xHeapU", "Bytes (MB)");
		outputImage(image, absImgDir + "Reducer-xHeapU.png");
		
	}

	private BufferedImage plotPNG(List<Pair> list, String title, String explain, String yTitle) {
		double[][] dataSet = new double[list.size()][2];
		for(int i = 0; i < list.size(); i++) {
			dataSet[i][0] = list.get(i).getReal();
			dataSet[i][1] = list.get(i).getEstimated();
		}
		
		JavaPlot p = new JavaPlot();
            
        p.setTitle(title);
        p.getAxis("x").setLabel("Job Number");//, "Arial", 20);
        p.getAxis("y").setLabel(yTitle); 
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_LEFT);
       
        //p.set("style", "histogram clustered");
		//p.set("style", "data histogram");
		//p.set("style", "fill solid 0.4 border");
		p.set("xrange", "[0:]");
		
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.setTitle("real " + explain);
        plot.getPlotStyle().setStyle(Style.LINES);
        //plot.getPlotStyle().setLineType(3);
        plot.set("using", "1");
        p.addPlot(plot);
        
        plot = new DataSetPlot(dataSet);
        plot.setTitle("estimated " + explain);
        plot.getPlotStyle().setStyle(Style.LINES);
        //plot.getPlotStyle().setLineType(3);
        plot.set("using", "2");
        p.addPlot(plot);

        PNGTerminal t = new PNGTerminal();  
        p.setTerminal(t);   
        p.setPersist(false);
        p.plot();
        
        return t.getImage();	
	}

	private SplitMapperJvmCostDiff readMapperArray(File diffMapperFile) {
		SplitMapperJvmCostDiff mDiff = new SplitMapperJvmCostDiff(diffMapperFile);
		return mDiff;
		
	}

	private SplitReducerJvmCostDiff readReducerArray(File diffReducerFile) {	
		SplitReducerJvmCostDiff rDiff = new SplitReducerJvmCostDiff(diffReducerFile);
		return rDiff;
	}

	
}
