package html.parser.link;

import html.util.HtmlFetcher;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;



public class LinksSaver {
	private String hostname;
	private String jobId;
	
	private String jobdetails_jsp;
	private String jobdetailshistory_jsp;
	private String jobhistory_jsp;
	private String jobconf_jsp;
	
	private String map_jobtasks_jsp;
	private List<String> map_tasks_list = new ArrayList<String>(); //refer to taskdetails_jsp
	
	private String reduce_jobtasks_jsp;
	private List<String> reduce_tasks_list = new ArrayList<String>(); //refer to taskdetails_jsp. list is empty if no reducer exists
	
	public LinksSaver (String hostname, String jobId) {
		this.hostname = hostname;
		this.jobId = jobId;
		jobdetails_jsp = "http://" + hostname + ":50030/jobdetails.jsp?jobid=" + jobId + "&refresh=0";
		jobhistory_jsp = "http://" + hostname + ":50030/jobhistory.jsp";
		jobdetailshistory_jsp = findDetailsHistoryJsp();
		jobconf_jsp = "http://" + hostname + ":50030/jobconf.jsp?jobid=" + jobId;
		
		map_jobtasks_jsp = "http://" + hostname + ":50030/jobtasks.jsp?jobid=" + jobId
				+ "&type=map&pagenum=1";
		reduce_jobtasks_jsp = "http://" + hostname + ":50030/jobtasks.jsp?jobid=" + jobId
				+ "&type=reduce&pagenum=1";
	}
	
	

	public String getMap_jobtasks_jsp() {
		return map_jobtasks_jsp;
	}

	public List<String> getMap_tasks_list() {
		return map_tasks_list;
	}

	public void setMap_tasks_list(List<String> map_tasks_list) {
		this.map_tasks_list = map_tasks_list;
	}

	public String getReduce_jobtasks_jsp() {
		return reduce_jobtasks_jsp;
	}
	
	public List<String> getReduce_tasks_list() {
		return reduce_tasks_list;
	}
	
	public void setReduce_tasks_list(List<String> reduce_tasks_list) {
		this.reduce_tasks_list = reduce_tasks_list;
	}
	
	public String getJobdetails_jsp() {
		return jobdetails_jsp;
	}

	public String getJobdetailshistory_jsp() {
		return jobdetailshistory_jsp;
	}

	public String getJobconf_jsp() {
		return jobconf_jsp;
	}

	//return e.g. http://master:50030/jobdetailshistory.jsp?jobid=job_201210101630_0003&logFile=file:/opt/hadooplog
	// /history/master_1349857853210_job_201210101630_0003_root_TeraSort%2B256MBno%2Breuse
	private String findDetailsHistoryJsp() {
		Document historyDoc = HtmlFetcher.getHtml(jobhistory_jsp);
		Element historyElem = historyDoc.getElementsMatchingOwnText(jobId).first();
		return historyElem.absUrl("href");

	}

	public void addMapTaskDetailsJsp(String mapTaskDetailsJsp) {		
		map_tasks_list.add(mapTaskDetailsJsp);
	}

	public void addReduceTaskDetailsJsp(String reduceTaskDetailsJsp) {
		reduce_tasks_list.add(reduceTaskDetailsJsp);	
	}



	public void setMap_jobtasks_jsp(String map_jobtasks_jsp) {
		this.map_jobtasks_jsp = map_jobtasks_jsp;
	}



	public void setReduce_jobtasks_jsp(String reduce_jobtasks_jsp) {
		this.reduce_jobtasks_jsp = reduce_jobtasks_jsp;
	}
	
	
}
/*
class TaskLinks {
	private String taskdetails_jsp; //point to current task
	private String taskPerf_jsp; // point to metrics/Coutners/JVM
	private String tasklog_jsp; //point to log
	private String taskstats_jsp; // point to final Counters
	public String getTaskdetails_jsp() {
		return taskdetails_jsp;
	}
	public void setTaskdetails_jsp(String taskdetails_jsp) {
		this.taskdetails_jsp = taskdetails_jsp;
	}
	public String getTaskPerf_jsp() {
		return taskPerf_jsp;
	}
	public void setTaskPerf_jsp(String taskPerf_jsp) {
		this.taskPerf_jsp = taskPerf_jsp;
	}
	public String getTasklog_jsp() {
		return tasklog_jsp;
	}
	public void setTasklog_jsp(String tasklog_jsp) {
		this.tasklog_jsp = tasklog_jsp;
	}
	public String getTaskstats_jsp() {
		return taskstats_jsp;
	}
	public void setTaskstats_jsp(String taskstats_jsp) {
		this.taskstats_jsp = taskstats_jsp;
	}
}
*/
