package verification.gnuplot;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.Style;

public class DiffDataflowPlot {

	private String jobName;
	private String baseDir;
	
	public DiffDataflowPlot(String jobName, String baseDir) {
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
		DiffDataflowPlot diffDataPlot = new DiffDataflowPlot(jobName, baseDir);
		diffDataPlot.visualize();	
	}

	public void visualize() {
		String compJvmCostDir = baseDir + jobName + "/compDataflow/";
		String absolErrorImageDir = baseDir + jobName + "/ErrorImage/dataError/";
		
		String diffMapper = "compMappers.txt";
		String diffReducer = "compReducers.txt";
		
		
		File diffMapperFile = new File(compJvmCostDir + diffMapper);
		File diffReducerFile = new File(compJvmCostDir + diffReducer);
		
	
		MapperDataDiff mDiff = readMapperArray(diffMapperFile);
		ReducerDataDiff rDiff = readReducerArray(diffReducerFile);
		
		plotMapperDiff(mDiff, absolErrorImageDir);
		plotReducerDiff(rDiff, absolErrorImageDir);	
		
		System.out.println("Finish ploting Dataflow Error");
		
	}

	private void plotMapperDiff(MapperDataDiff mDiff, String absolErrorImageDir) {
	
		List<Float> xRecBMList = mDiff.getxRecBM();
		List<Float> xBytesBMList = mDiff.getxRawBM();
		List<Float> xRecAMList = mDiff.getxRecAM();
		List<Float> xBytesAMList = mDiff.getxRawAM();
		
		File imgDir = new File(absolErrorImageDir);
		if(!imgDir.exists())
			imgDir.mkdirs();
		
		BufferedImage image = plotPNG(xRecBMList, "Mapper xRecords Before Merge", "xRecBM", "Difference (M)");
		outputImage(image, absolErrorImageDir + "Mapper-abs-xRecBM.png");
		image = plotPNG(xBytesBMList, "Mapper xBytes Before Merge", "xBytesBM", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Mapper-abs-xBytesBM.png");
		image = plotPNG(xRecAMList, "Mapper xRecords After Merge", "xRecAM", "Difference (M)");
		outputImage(image, absolErrorImageDir + "Mapper-abs-xRecAM.png");
		image = plotPNG(xBytesAMList, "Mapper xBytes After Merge", "xBytesAM", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Mapper-abs-xBytesAM.png");
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

	private void plotReducerDiff(ReducerDataDiff rDiff, String absolErrorImageDir) {
						
		List<Float> xShCompMBList = rDiff.getxShCompMB();
		List<Float> xShRawMBList = rDiff.getxShRawMB();
		List<Float> xInRecList = rDiff.getxInRec();
		List<Float> xInputMBList = rDiff.getxInputMB();
		List<Float> xOutRecList = rDiff.getxOutRec();
		List<Float> xOutMBList = rDiff.getxOutMB();
		
		BufferedImage image = plotPNG(xShCompMBList, "Max Shuffle Compressed Bytes", "xShCompMB", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xShCompMB.png");
		image = plotPNG(xShRawMBList, "Max Shuffle Raw Bytes", "xShRawMB", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xShRawMB.png");
		image = plotPNG(xInRecList, "Max Reduce Input Records", "xInRec", "Difference (M)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xInRec.png");
		image = plotPNG(xInputMBList, "Max Reduce Input Bytes", "xInputMB", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xInputMB.png");
		image = plotPNG(xOutRecList, "Max Reduce Output Records", "xOutRec", "Difference (M)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xOutRec.png");
		image = plotPNG(xOutMBList, "Max Reduce Output Bytes", "xOutMB", "Difference (MB)");
		outputImage(image, absolErrorImageDir + "Reducer-abs-xOutMB.png");
	}

	private BufferedImage plotPNG(List<Float> rtList, String title, String explain, String yTitle) {
		double[][] dataSet = new double[rtList.size()][1];
		for(int i = 0; i < rtList.size(); i++)
			dataSet[i][0] = rtList.get(i);
		
		JavaPlot p = new JavaPlot();
            
        p.setTitle(title);
        p.getAxis("x").setLabel("Job Number");//, "Arial", 20);
        p.getAxis("y").setLabel(yTitle); 
        //p.getAxis("x").setBoundaries(0, list.get(list.size() - 1).getTime());
        p.setKey(JavaPlot.Key.TOP_RIGHT);
       
        p.set("style", "histogram clustered");
		p.set("style", "data histogram");
		p.set("style", "fill solid 0.4 border");
		p.set("xrange", "[0:]");
		
        DataSetPlot plot = new DataSetPlot(dataSet);
        plot.setTitle(explain);
        plot.getPlotStyle().setStyle(Style.BOXES);
        plot.getPlotStyle().setLineType(3);
        plot.set("using", "1");
        p.addPlot(plot);

        PNGTerminal t = new PNGTerminal();  
        p.setTerminal(t);   
        p.setPersist(false);
        p.plot();
        
        return t.getImage();	
	}

	private MapperDataDiff readMapperArray(File diffMapperFile) {
		MapperDataDiff mDiff = new MapperDataDiff(diffMapperFile);
		return mDiff;
		
	}

	private ReducerDataDiff readReducerArray(File diffReducerFile) {	
		ReducerDataDiff rDiff = new ReducerDataDiff(diffReducerFile);
		return rDiff;
	}

}
