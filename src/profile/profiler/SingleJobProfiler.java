package profile.profiler;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import profile.job.Job;
import profile.job.JobTasksParser;
import profile.task.mapper.Mapper;
import profile.task.reducer.Reducer;

import html.parser.job.JobConfigurationParser;
import html.parser.job.JobDetailedHistoryParser;
import html.parser.job.JobDetailsParser;
import html.parser.link.LinksSaver;
import html.parser.task.MapTaskParser;
import html.parser.task.ReduceTaskParser;

public class SingleJobProfiler {
	private Job job;
	private LinksSaver linksSaver;
	
	private boolean needMetrics = true; //going to analyze task counters/metrics/jvm?
	
	private int sampleMapperNum = 0; // Only analyze sampleMapperNum mappers, 0 represents all mappers will be analyzed
	private int sampleReducerNum = 0; // Only analyze sampleReducerNum reducers, 0 represents all reducers will be analyzed
	
	public SingleJobProfiler (String hostname, String jobId) {
		job = new Job();
		linksSaver = new LinksSaver(hostname, jobId);
	}
	
	public SingleJobProfiler (String hostname, String jobId, boolean needMetrics) {
		this(hostname, jobId);
		this.needMetrics = needMetrics;
	}
	
	public SingleJobProfiler (String hostname, String jobId, boolean needMetrics, int sampleMapperNum, int sampleReducerNum) {
		this(hostname, jobId);
		this.needMetrics = needMetrics;
		this.sampleMapperNum = sampleMapperNum;
		this.sampleReducerNum = sampleReducerNum;
	}
	
	public Job profile() {
		
		String jobdetaisUrl = linksSaver.getJobdetails_jsp();
		Document wholeJspDoc = html.util.HtmlFetcher.getHtml(jobdetaisUrl);
		Element body = wholeJspDoc.getElementsByTag("body").first();
		
		Element statusElem = body.select(":containsOwn(Status:)").first();
		String status = statusElem.nextSibling().toString().trim();
		if(status.equals("Failed"))
			return null;
		
		JobDetailsParser.parseJobDetails(linksSaver, job); // get Job Counters, FileSystemCounters and Map-Reduce Framework
		JobDetailedHistoryParser.parseJobDetailedHistory(linksSaver, job); //get Job start/end times and map/reduce phase start/end times
		JobConfigurationParser.parseJobConf(linksSaver, job); //get the configuration of this job
		JobTasksParser.parseJobTasks(linksSaver); //initiate map/reduce tasks link list
		
		parseMapperTasks();
		parseReducerTasks();
		
		return job;
		
	}
	
	private void parseMapperTasks() {
		assert(sampleMapperNum >= 0);
		if(sampleMapperNum == 0)
			sampleMapperNum = Integer.MAX_VALUE;		
		
		for(int i = 0; i < linksSaver.getMap_tasks_list().size() && i < sampleMapperNum; i++) {
			Mapper newMapper = MapTaskParser.parseMapTask(linksSaver.getMap_tasks_list().get(i), needMetrics);
			job.addMapper(newMapper);
		}
		
	}

	private void parseReducerTasks() {
		assert(sampleReducerNum >= 0);
		if(sampleReducerNum == 0)
			sampleReducerNum = Integer.MAX_VALUE;
		
		for(int i = 0; i < linksSaver.getReduce_tasks_list().size() && i < sampleReducerNum; i++) {
			Reducer newReducer = ReduceTaskParser.parseReduceTask(linksSaver.getReduce_tasks_list().get(i), needMetrics);
			job.addReducer(newReducer);
		}	
	}

	
	public static void main(String[] args) {
		String jobId = "job_201210172333_0001";
		String hostname = "m105";
		
		boolean needMetrics = true; //going to analyze task counters/metrics/jvm?
		int sampleMapperNum = 0;
		int sampleReducerNum = 0;
		
		SingleJobProfiler profiler = new SingleJobProfiler(hostname, jobId, needMetrics, sampleMapperNum, sampleReducerNum);

		Job job = profiler.profile();
		

	}
}
