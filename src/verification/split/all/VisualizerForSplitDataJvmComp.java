package verification.split.all;

import verification.split.gnuplot.SplitDiffDataflowPlot;
import verification.split.gnuplot.SplitDiffJvmCostPlot;
import verification.split.jvmcost.SplitJvmCostComparator;
import verification.split.dataflow.SplitDataflowComparator;

public class VisualizerForSplitDataJvmComp {

	public static void main(String[] args) {
		/*********************************Big Experiments*********************************/
		//String jobName = "Big-uservisits_aggre-pig-50G";
		//String jobName = "BigBuildInvertedIndex";
		//String jobName = "BigTeraSort-36GB";
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "BigTwitterInDegreeCount";
		//String jobName = "BigWiki-m36-r18";
		
		//String baseDir = "/home/xulijie/MR-MEM/BigExperiments/";
		/*********************************Big Experiments*********************************/
		
		/*********************************Sample Experiments*********************************/
		//String jobName = "SampleBuildInvertedIndex-1G";
		//String jobName = "SampleTeraSort-1G";
		//String jobName = "SampleTwitterBiDirectEdgeCount";
		//String jobName = "SampleTwitterInDegreeCount";
		//String jobName = "SampleUservisits-1G";
		String jobName = "SampleWikiWordCount-1G";
		
		String baseDir = "/home/xulijie/MR-MEM/SampleExperiments/";

		/*********************************Sample Experiments*********************************/
		
		SplitDataflowComparator dataComp = new SplitDataflowComparator(jobName, baseDir);
		dataComp.compareDataflow();
		
		SplitJvmCostComparator jvmComp = new SplitJvmCostComparator(jobName, baseDir);
		jvmComp.compareJvmCost();
		
		SplitDiffDataflowPlot dataPlot = new SplitDiffDataflowPlot(jobName, baseDir);
		dataPlot.visualize();
		
		SplitDiffJvmCostPlot jvmPlot = new SplitDiffJvmCostPlot(jobName, baseDir);
		jvmPlot.visualize();
		
		
		
		System.out.println("Finished!");	
	}
}
