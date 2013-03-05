package html.parser.job;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.job.Job;

import html.parser.link.LinksSaver;
import html.util.DateParser;
import html.util.HtmlFetcher;

public class JobDetailedHistoryParser {

	public static void parseJobDetailedHistory(LinksSaver linksSaver, Job job) {
		String jobdetaisUrl = linksSaver.getJobdetailshistory_jsp();
		
		Document jobDetailsHistory = HtmlFetcher.getHtml(jobdetaisUrl);
		Element jobNameElem = jobDetailsHistory.getElementsContainingOwnText("JobName:").first();
		String jobName = jobNameElem.nextSibling().toString().trim();
		job.setJobName(jobName);
		
		Element jobSubmittedTimeElem = jobDetailsHistory.getElementsContainingOwnText("Submitted At:").first();
		String jobSubmittedTime = jobSubmittedTimeElem.nextSibling().toString();
		long jobSubmittedTimeMS = DateParser.parseJobStartFinishTimeMS(jobSubmittedTime);
		//System.out.println("Submitted Time = " + jobSubmittedTimeMS);
		
		Element jobStartTimeElem = jobDetailsHistory.getElementsContainingOwnText("Launched At:").first();
		String jobStartTime = jobStartTimeElem.nextSibling().toString();
		jobStartTime = jobStartTime.substring(0, jobStartTime.indexOf('(')).trim();
		long jobStartTimeMS = DateParser.parseJobStartFinishTimeMS(jobStartTime);
		job.setJobStartTimeMS(jobStartTimeMS);
		
		Element jobStopTimeElem = jobDetailsHistory.getElementsContainingOwnText("Finished At:").first();
		String jobStopTime = jobStopTimeElem.nextSibling().toString();
		jobStopTime = jobStopTime.substring(0, jobStopTime.indexOf('(')).trim();
		long jobStopTimeMS = DateParser.parseJobStartFinishTimeMS(jobStopTime);
		job.setJobStopTimeMS(jobStopTimeMS);
		
		Elements trs = jobDetailsHistory.getElementsByTag("tbody").first().children();
		for(int i = 1; i < trs.size(); i++) {
			Element tr = trs.get(i);
			String kind = tr.child(0).text();
			int totalTasksNum = Integer.parseInt(tr.child(1).text());
			int successfulTasksNum = Integer.parseInt(tr.child(2).text());
			int failedTasksNum = Integer.parseInt(tr.child(3).text());
			int killedTasksNum = Integer.parseInt(tr.child(4).text());
			String startTime = tr.child(5).text();
			String finishTime = tr.child(6).text();
			long phaseStartTimeMS = DateParser.parseJobStartFinishTimeMS(startTime.trim());
			long phaseStopTimeMS = DateParser.parseJobStartFinishTimeMS(finishTime.substring(0, finishTime.indexOf('(')).trim());
			
			//System.out.println(kind + " " + totalTasksNum + " " + successfulTasksNum + " " + failedTasksNum
			//		+ " " + killedTasksNum + " " + phaseStartTimeMS + " " + phaseStopTimeMS);
			job.setPhaseItem(kind, totalTasksNum, successfulTasksNum, failedTasksNum, killedTasksNum, phaseStartTimeMS, phaseStopTimeMS);
		}
	}
}
