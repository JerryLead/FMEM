package verification.split.all;

import verification.split.gnuplot.SplitDiffDataflowPlot;
import verification.split.gnuplot.SplitDiffJvmCostPlot;
import verification.split.jvmcost.SplitJvmCostComparator;
import verification.split.dataflow.SplitDataflowComparator;

public class STBVisualizerForSplitDataJvmComp {

	public static void main(String[] args) {
		/*********************************Big Experiments*********************************/
		String bigJobName = "Big-uservisits_aggre-pig-50G";
		
		//String bigJobName = "BigBuildInvertedIndex";
		//String bigJobName = "BigTeraSort-36GB";
		//String bigJobName = "BigTwitterBiDirectEdgeCount";
		//String bigJobName = "BigTwitterInDegreeCount";
		//String bigJobName = "BigWiki-m36-r18";
		
		String bigBaseDir = "G:\\MR-MEM\\BigExperiments\\";
		/*********************************Big Experiments*********************************/
		
		/*********************************Sample Experiments*********************************/
		
		String sampleJobName = "SampleToBig-uservisits_aggre-pig-50G";
		//String sampleJobName = "SampleToBigBuildInvertedIndex";
		//String sampleJobName = "SampleToBigTeraSort-36GB";
		//String sampleJobName = "SampleToBigTwitterBiDirectEdgeCount";
		//String sampleJobName = "SampleToBigTwitterInDegreeCount";
		//String sampleJobName = "SampleToBigWiki-m36-r18";
		
		String sampleBaseDir = "G:\\MR-MEM\\CompExperiments\\";

		/*********************************Sample Experiments*********************************/
		
		SplitDataflowComparator dataComp = new SplitDataflowComparator(sampleJobName, sampleBaseDir, bigJobName, bigBaseDir);
		dataComp.compareDataflow();
		
		SplitJvmCostComparator jvmComp = new SplitJvmCostComparator(sampleJobName, sampleBaseDir, bigJobName, bigBaseDir);
		jvmComp.compareJvmCost();
		
		SplitDiffDataflowPlot dataPlot = new SplitDiffDataflowPlot(sampleJobName, sampleBaseDir);
		dataPlot.visualize();
		
		SplitDiffJvmCostPlot jvmPlot = new SplitDiffJvmCostPlot(sampleJobName, sampleBaseDir);
		jvmPlot.visualize();
		
		
		
		System.out.println("Finished!");	
	}
}
