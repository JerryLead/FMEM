package html.parser.job;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import profile.job.Job;

import html.util.HtmlFetcher;
import html.parser.link.LinksSaver;

public class JobConfigurationParser {

	public static void parseJobConf(LinksSaver linksSaver, Job job) {
		String jobConfLink = linksSaver.getJobconf_jsp();
		Document wholeJspDoc = HtmlFetcher.getHtml(jobConfLink);
		Element body = wholeJspDoc.getElementsByTag("body").first();

		Element confTable = body.getElementsByTag("tbody").first();
		for (Element elem : confTable.children()) {
			if (!elem.child(0).text().equals("name"))
				job.addConfItem(elem.child(0).text(), elem.child(1).text());
		}
	}

}
