package memory.model.reducer;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.reducer.Reduce;
import profile.task.reducer.Reducer;
import profile.task.reducer.ReducerBuffer;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.JvmMetricsUtil;
import memory.model.jvm.JvmModel;
import memory.model.jvm.ReducerEstimatedJvmCost;

public class ReduceJvmModel {

	private Configuration fConf;
	private Configuration newConf;
	private InitialJvmCapacity gcCap;
	
	public ReduceJvmModel(Configuration fConf, Configuration newConf, InitialJvmCapacity gcCap) {
		this.fConf = fConf;
		this.newConf = newConf;
		this.gcCap = gcCap;
	}

	public void estimateJvmCost(Reducer fReducer, Reducer eReducer, ReducerEstimatedJvmCost eSSJvmModel) {
		long sortFinishedTimeMS = fReducer.getSortPhaseFinishTimeMS();
		//ensure the fRedMax doesn't contain metrics in sort phase
		sortFinishedTimeMS += Integer.parseInt(fConf.getConf("child.monitor.jstat.seconds")) * 1500; 
		
		List<JstatItem> jstatItemList = fReducer.getMetrics().getJstatMetricsList();
		List<JvmItem> jvmItemList = fReducer.getMetrics().getJvmMetricsList();
		
		int sortJstatIndex = JvmMetricsUtil.getJstatItem(jstatItemList, sortFinishedTimeMS);
		int sortJvmIndex = JvmMetricsUtil.getJvmItem(jvmItemList, sortFinishedTimeMS);
		
		//find the max value in shuffle-sort phase
		JvmModel fRedMax = JvmMetricsUtil.maxJstatValue(jstatItemList, sortJstatIndex, jstatItemList.size() - 1);
		
		JvmMetricsUtil.updateMaxJvmValue(fRedMax, jvmItemList, sortJvmIndex, jvmItemList.size() - 1);
		
		// estimate the Old Generation
		float fOldUsed = fRedMax.getOldUsed();
		
		//float fFix = fOldUsed - jstatItemList.get(sortJstatIndex).getOU(); // need more consideration
		// assume there are no fix bytes allocated in Reduce Phase
		float fFix = computeFix(jstatItemList, sortJstatIndex);
		float fTempObj = computeTempObj(jstatItemList.subList(sortJstatIndex, jstatItemList.size()), fRedMax, fReducer);
		
		float eFix = fFix;
		float eTempObj = computeReduceTempObj(fReducer, eReducer, fTempObj);
			
		computeReduceJvmCost(fReducer, eReducer, eTempObj, eFix, fRedMax, eSSJvmModel);	
	}

	private float computeFix(List<JstatItem> jstatItemList, int sortJstatIndex) {
		
		return 0;
	}

	private void computeReduceJvmCost(Reducer fReducer, Reducer eReducer, float eTempObj, float eFix, JvmModel fRedMax, ReducerEstimatedJvmCost eSSJvmModel) {
		
		float fInMemSegBytesAfterSortMB = (float) ((double)fReducer.getSort().getMixSortMerge().getInMemorySegmentsSize() / 1024 / 1024);
		float fReduceInputBytesMB = (float) ((double)fReducer.getReduce().getInputBytes() / 1024 / 1024);
		
		float eInMemSegBytesAfterSortMB = (float) ((double)eReducer.getSort().getMixSortMerge().getInMemorySegmentsSize() / 1024 / 1024);
		float eReduceInputBytesMB = (float) ((double)eReducer.getReduce().getInputBytes() / 1024 / 1024);
		
		float eOGCMX = gcCap.geteOGCMX();
		float enOU;  //estimated minimum usage of old generation
		float exOU;  //estimated maximum usage of old generation
		float eNewU; //estimated usage of new generation
		String redReason;
		
		if(eTempObj <= gcCap.geteEC()) {
			if(eSSJvmModel.getxOU() == 0 && eSSJvmModel.getNewUsed() + eTempObj <= gcCap.geteEC()) {
				enOU = 0;
				exOU = 0;
				eNewU = eSSJvmModel.getNewUsed() + eTempObj;
				redReason = "reduce: eSSJvmModel.getxOU() == 0 && eSSJvmModel.getNewUsed() + eTempObj <= gcCap.geteEC()";
			}
			else {
				eNewU = eTempObj;
				enOU = 0;
				exOU = 0;
				redReason = "reduce: eTempObj <= gcCap.geteEC()";
			}
		}
		else {
			eNewU = Math.min(eTempObj, gcCap.geteNGCMX());
			enOU = 0;//eReduceInputBytesMB;
			exOU = eReducer.getSort().getMixSortMerge().getInMemorySegmentsSize();
					//+ 2 * eReducer.getSort().getMixSortMerge().getOnDiskSegmentsSize();
			
			enOU = Math.min(eOGCMX, enOU + eFix); 
			exOU = Math.min(eOGCMX, exOU + eFix);
			redReason = "reduce: eOU = eReduceInputBytesMB, eNewU = min(eTempObj, eNGCMX)";
				
			
		}	
		
		//No GC occurs in shuffle & sort phase
		/*
		if(eSSJvmModel.getnOU() == 0 && eSSJvmModel.getNewUsed() + eTempObj <= gcCap.geteEC()) {
			eNewU = eSSJvmModel.getNewUsed() + eTempObj;
			enOU = 0;
			exOU = 0;	
			redReason = "ssOU = 0 && ssNGU + eTempObj <= eEC";
		}
		
		else {
			eNewU = Math.min(eTempObj, gcCap.geteNGCMX());
			enOU = eReduceInputBytesMB;
			exOU = eReducer.getSort().getMixSortMerge().getInMemorySegmentsSize() 
					+ 2 * eReducer.getSort().getMixSortMerge().getOnDiskSegmentsSize();
			
			enOU = Math.min(eOGCMX, enOU + eFix); 
			exOU = Math.min(eOGCMX, exOU + eFix);
			redReason = "eOU = eReduceInputBytesMB, eNewU = min(eTempObj, eNGCMX)";
		
		}		
		*/
		// estimate the Perm Generation
		float ePermUsed = fRedMax.getPermUsed();	
		
		
		eSSJvmModel.setRedPermUsed(ePermUsed);
		eSSJvmModel.setRedNewUsed(eNewU);
		eSSJvmModel.setRednOU(enOU);
		eSSJvmModel.setRedxOU(exOU);
		eSSJvmModel.setRednHeapU(enOU + eNewU);
		eSSJvmModel.setRedxHeapU(exOU + eNewU);
		eSSJvmModel.setRedTempObj(eTempObj);	
		eSSJvmModel.setRedFix(eFix);
		eSSJvmModel.setRedReason(redReason);
		eSSJvmModel.setReduceInputBytes(eReduceInputBytesMB);

	}

	private float computeReduceTempObj(Reducer fReducer, Reducer eReducer, float fTempObj) {
		/*
		long fReduceInputBytes = fReducer.getReduce().getInputBytes();
		long eReduceInputBytes = eReducer.getReduce().getInputBytes();
		
		double eTempObj = (double)eReduceInputBytes / fReduceInputBytes * fTempObj;
		
		return (float) (eTempObj);	
		*/
		long fReduceInputRecords = fReducer.getReducerCounters().getReduce_input_records();
		long eReduceInputRecords = eReducer.getReducerCounters().getReduce_input_records();
		
		double eTempObj = (double)eReduceInputRecords / fReduceInputRecords * fTempObj;
		
		return (float) (eTempObj);
	}

	private float computeTempObj(List<JstatItem> subList, JvmModel fRedMax, Reducer fReducer) {
		float tempObj = 0;
		//float inMemSegBytesAfterSortMB = (float) ((double)fReducer.getSort().getMixSortMerge().getInMemorySegmentsSize() / 1024 / 1024);
			
		JstatItem lastJstat = subList.get(subList.size() - 1);
		if(lastJstat.getFGC() == 0 && lastJstat.getYGC() == 0) 
			tempObj = fRedMax.getNewUsed() - subList.get(0).getNGU();
			
		else {
			float ascend = 0;
			float descend = 0;
			int size = subList.size();
			
			float[] c = new float[size];
			
			for(int i = 0; i < size; i++) {
				JstatItem item = subList.get(i);
				c[i] = item.getEU() + item.getS0U() + item.getS1U(); //only consider the New Generation
			}
			
			//ascend += c[0];
			for(int i = 1; i < c.length; i++) {
				if(c[i] > c[i-1])
					ascend += c[i] - c[i-1];  // contains InMemorySegments and accompanying temp objects 
				
				else if(c[i] < c[i-1]) {
					descend += (c[i-1] - c[i]); // revoke InMemorySegments(merged to disk) and temp objects
				}
			}
			
			
			tempObj = ascend; // conservative
		}
					
		return tempObj;
	
	}

	

}
