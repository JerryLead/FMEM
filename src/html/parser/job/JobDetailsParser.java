package html.parser.job;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import profile.job.Job;


import html.parser.link.LinksSaver;
import html.util.HtmlFetcher;

public class JobDetailsParser {
	
	/**
	 * parse the given html (jobdetails.jsp) and set Job Counters, FileSystemCounters and 
	 * Map-Reduce Framework of Job class. 
	 * 
	 * @param link e.g. http://master:50030/jobdetails.jsp?jobid=job_201210101630_0001&refresh=0
	 * @param job
	 */
	public static void parseJobDetails (LinksSaver link, Job job) {
		String jobdetaisUrl = link.getJobdetails_jsp();
		
		Document jobdetais = HtmlFetcher.getHtml(jobdetaisUrl);
		Element countersTable = jobdetais.getElementsByTag("p").get(0).nextElementSibling();
		
		setJobCounters(countersTable, job);
	}

	private static void setJobCounters(Element countersTable, Job job) {
		Elements trs = countersTable.getElementsByTag("tbody").first().children();
		String rowspanName = "";
		for(int i = 1; i < trs.size(); i++) {
			Element tr = trs.get(i);
			String counterName = "";
			long[] counterValues = new long[3]; // map, reduce and total
			
			if(tr.child(0).hasAttr("rowspan")) {
				rowspanName = tr.child(0).text().trim();
				counterName = tr.child(1).text();
				counterValues[0] = Long.parseLong(tr.child(2).text().replaceAll(",", ""));
				counterValues[1] = Long.parseLong(tr.child(3).text().replaceAll(",", ""));
				counterValues[2] = Long.parseLong(tr.child(4).text().replaceAll(",", ""));
			}
			else if (tr.children().size() == 4) {
				counterName = tr.child(0).text();
				counterValues[0] = Long.parseLong(tr.child(1).text().replaceAll(",", ""));
				counterValues[1] = Long.parseLong(tr.child(2).text().replaceAll(",", ""));
				counterValues[2] = Long.parseLong(tr.child(3).text().replaceAll(",", ""));
			}
			
			job.addJobCountersItem(rowspanName, counterName, counterValues);
		}
	}
	

}
