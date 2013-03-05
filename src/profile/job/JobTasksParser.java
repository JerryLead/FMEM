package profile.job;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import html.util.HtmlFetcher;
import html.parser.link.LinksSaver;

/*
 * Given "http://master:50030/jobtasks.jsp?jobid=job_201210101630_0001&type=map&pagenum=1" and
 * "http://master:50030/jobtasks.jsp?jobid=job_201210101630_0001&type=reduce&pagenum=1", this class
 * parses the htmls and finds out each mapper/reducer's taskdetail.jsp link.
 */
public class JobTasksParser {

	public static void parseJobTasks(LinksSaver linksSaver) {
		initMapTasksList(linksSaver);
		initReduceTasksList(linksSaver);
		
	}
	

	private static void initMapTasksList(LinksSaver linksSaver) {
		String jobtaskUrl = linksSaver.getMap_jobtasks_jsp();
		Document jobtaskDoc = HtmlFetcher.getHtml(jobtaskUrl);
		Elements mapTrs = jobtaskDoc.getElementsByTag("tbody").first()
				.children();
		
		for (int i = 1; i < mapTrs.size(); i++) {
			Element tr = mapTrs.get(i);
			//http://master:50030/taskdetails.jsp?jobid=job_201208242049_0013&tipid=task_201208242049_0013_m_000000
			String mapTaskUrl = tr.child(0).child(0).absUrl("href");
			linksSaver.addMapTaskDetailsJsp(mapTaskUrl);
		}
		
	}
	
	private static void initReduceTasksList(LinksSaver linksSaver) {
		String jobtaskUrl = linksSaver.getReduce_jobtasks_jsp();
		Document jobtaskDoc = HtmlFetcher.getHtml(jobtaskUrl);
		if(jobtaskDoc.getElementsByTag("tbody") == null)
			return;
		Elements reduceTrs = jobtaskDoc.getElementsByTag("tbody").first()
				.children();
		
		for (int i = 1; i < reduceTrs.size(); i++) {
			Element tr = reduceTrs.get(i);
			//http://master:50030/taskdetails.jsp?jobid=job_201210101630_0001&tipid=task_201210101630_0001_r_000000
			String reduceTaskUrl = tr.child(0).child(0).absUrl("href");
			linksSaver.addReduceTaskDetailsJsp(reduceTaskUrl);
		}
	}


}
