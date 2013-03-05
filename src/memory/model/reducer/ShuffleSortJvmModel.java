package memory.model.reducer;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.reducer.Reducer;
import profile.task.reducer.ReducerBuffer;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.ReducerEstimatedJvmCost;
import memory.model.jvm.JvmMetricsUtil;
import memory.model.jvm.JvmModel;

public class ShuffleSortJvmModel {

	private Configuration fConf;
	private Configuration newConf;
	private InitialJvmCapacity gcCap;
	
	public ShuffleSortJvmModel(Configuration fConf, Configuration newConf, InitialJvmCapacity gcCap) {
		this.fConf = fConf;
		this.newConf = newConf;
		this.gcCap = gcCap;
	}

	public ReducerEstimatedJvmCost estimateJvmCost(Reducer fReducer, Reducer eReducer) {
		
		long sortFinishedTimeMS = fReducer.getSortPhaseFinishTimeMS();
		
		List<JstatItem> jstatItemList = fReducer.getMetrics().getJstatMetricsList();
		List<JvmItem> jvmItemList = fReducer.getMetrics().getJvmMetricsList();
		
		int sortJstatIndex = JvmMetricsUtil.getJstatItem(jstatItemList, sortFinishedTimeMS);
		int sortJvmIndex = JvmMetricsUtil.getJvmItem(jvmItemList, sortFinishedTimeMS);
		
		// find the max value in shuffle-sort phase
		JvmModel fSSPhaseMax = JvmMetricsUtil.maxJstatValue(jstatItemList, 0, sortJstatIndex);
		
		JvmMetricsUtil.updateMaxJvmValue(fSSPhaseMax, jvmItemList, 0, sortJvmIndex);
			
		// estimate the Old Generation
		float fOldUsed = fSSPhaseMax.getOldUsed();
		// float fHeapUsed = fSSPhaseMax.getHeapUsed();

		// always equals 0. Actually, it's hard to estimate the fix bytes in reducer task, we can let users to set this value
		float fFix = computeFix(fOldUsed, fReducer); //seems reasonable
		
		float fTempObj = computeTempObj(jstatItemList.subList(0, sortJstatIndex + 1), fSSPhaseMax, fReducer);
		
		float eTempObj = computeShuffleSortTempObj(fReducer, eReducer, fTempObj);
		float eFix = fFix;
		
		return computeShuffleSortJvmCost(fReducer, eReducer, eTempObj, eFix, fSSPhaseMax);	
	}

	

	private float computeFix(float fOldUsed, Reducer fReducer) {
		float fShuffleBytesMB = (float) ((double)fReducer.getReducerCounters().getReduce_shuffle_raw_bytes() / (1024 * 1024));
		float fix = 0;
		
		if(fOldUsed > fShuffleBytesMB)
			fix = fOldUsed - fShuffleBytesMB;
		return fix;
	}

	private float computeShuffleSortTempObj(Reducer fReducer, Reducer eReducer, float fTempObj) {
		long fShuffleBytes = fReducer.getReducerCounters().getReduce_shuffle_raw_bytes();
		long eShuffleBytes = eReducer.getReducerCounters().getReduce_shuffle_raw_bytes();
		
		double eTempObj = (double)eShuffleBytes / fShuffleBytes * fTempObj;
		
		return (float) (eTempObj);		
	}
/*
	private ReducerEstimatedJvmCost computeShuffleSortJvmCost(Reducer fReducer, Reducer eReducer, float eTempObj, float eFix, JvmModel fSSPhaseMax) {
	    //[-----------------Runtime().maxMemory()-----------------------]
	    //[-------------inMemSegBuffer 70%---------------][--for other--]
		//[------mergeBuffer 66%-------][--Write Buffer--]
		ReducerBuffer eBuffer = eReducer.getReducerBuffer();
		//float runtime_maxMemory_MB = eBuffer.getRuntime_maxMemoryMB(); 
		float inMemSegBuffer = eBuffer.getMemoryLimitMB();
		float mergeBuffer = eBuffer.getInMemoryBufferLimitMB();
		
		//float fShuffleBytesMB = (float) ((double)fReducer.getReducerCounters().getReduce_shuffle_bytes() / (1024 * 1024));
		float eShuffleBytesMB = (float) ((double)eReducer.getReducerCounters().getReduce_shuffle_bytes() / (1024 * 1024));
		
		float eOGCMX = gcCap.geteOGCMX();
		float enOU;  //estimated minimum usage of old generation
		float exOU;  //estimated maximum usage of old generation
		float eNewU; //estimated usage of new generation
		String ssReason;
		
		//new generation can hold all the shuffle bytes and accompanying temp objects
		if(eShuffleBytesMB + eTempObj <= gcCap.geteNGCMX()) {
			enOU = 0;
			exOU = eShuffleBytesMB;
			eNewU = eShuffleBytesMB + eTempObj;
			ssReason = "eShuffleBytesMB + eTempObj <= eNGCMX";
		}
		
		else {
			
			if(eOGCMX < mergeBuffer) {
				System.err.println("eOGCMX is too small! eOGCMX = " + eOGCMX + ", mergeBuffer = " + mergeBuffer);
			}
			
			eNewU = gcCap.geteNGCMX();
			
			//less than mergeBuffer
			if(eShuffleBytesMB <= mergeBuffer) {
				enOU = eShuffleBytesMB;
				exOU = eShuffleBytesMB;
				ssReason = "eShuffleBytesMB <= mergeBuffer";
			}
			
			//more than mergeBuffer, less than inMemSegBuffer
			else if(eShuffleBytesMB <= inMemSegBuffer) {
				//merge will occur during shuffling map outputs into inMemSegBuffer
				// eOldU >= mergeBuffer && eOldU <= eShuffleBytesMB
				enOU = mergeBuffer;
				exOU = Math.min(eOGCMX, eShuffleBytesMB); //eOGCMX >= mergeBuffer
				ssReason = "mergeBuffer < eShuffleBytesMB <= inMemSegBuffer";
			}
			
			//more than inMemSegBuffer
			else {
				enOU = Math.min(eOGCMX, inMemSegBuffer);
				exOU = Math.min(eOGCMX, eShuffleBytesMB);
				ssReason = "eShuffleBytesMB > inMemSegBuffer";
			}
		}
		
		enOU = Math.min(eOGCMX, enOU + eFix);
		exOU = Math.min(eOGCMX, exOU + eFix); 
		

		// estimate the Perm Generation
		float ePermUsed = fSSPhaseMax.getPermUsed();	
		
		ReducerEstimatedJvmCost jvmCost = new ReducerEstimatedJvmCost();
		jvmCost.setSSPermUsed(ePermUsed);
		jvmCost.setSSNewUsed(eNewU);
		jvmCost.setSSnOU(enOU);
		jvmCost.setSSxOU(exOU);
		jvmCost.setSSnHeapU(enOU + eNewU);
		jvmCost.setSSxHeapU(exOU + eNewU);
		jvmCost.setSSTempObj(eTempObj);
		jvmCost.setSSFix(eFix);
		jvmCost.setReason(ssReason);
		
		return jvmCost;
		
	}
*/	
	private ReducerEstimatedJvmCost computeShuffleSortJvmCost(Reducer fReducer, Reducer eReducer, float eTempObj, float eFix, JvmModel fSSPhaseMax) {
	    //[-----------------Runtime().maxMemory()-----------------------]
	    //[-------------inMemSegBuffer 70%---------------][--for other--]
		//[------mergeBuffer 66%-------][--Write Buffer--]
		ReducerBuffer eBuffer = eReducer.getReducerBuffer();
		//float runtime_maxMemory_MB = eBuffer.getRuntime_maxMemoryMB(); 
		float inMemSegBuffer = eBuffer.getMemoryLimitMB();
		float mergeBuffer = eBuffer.getInMemoryBufferLimitMB();
		
		//float fShuffleBytesMB = (float) ((double)fReducer.getReducerCounters().getReduce_shuffle_bytes() / (1024 * 1024));
		float eShuffleBytesMB = (float) ((double)eReducer.getReducerCounters().getReduce_shuffle_raw_bytes() / (1024 * 1024));
		
		float eOGCMX = gcCap.geteOGCMX();
		float eOGC = gcCap.geteOGC();
		float eNGCMX = gcCap.geteNGCMX();
		float eEdenC = gcCap.geteEC();
		float xms = newConf.getXms();
		float enOU;  //estimated minimum usage of old generation
		float exOU;  //estimated maximum usage of old generation
		float eNewU; //estimated usage of new generation
		
		
		String ssReason;
		
		//new generation can hold all the shuffle bytes and accompanying temp objects
		if(eShuffleBytesMB + eTempObj < eEdenC) { //without YGC
			enOU = 0;
			exOU = 0;
			eNewU = eShuffleBytesMB + eTempObj;
			ssReason = "eShuffleBytesMB + eTempObj < eEdenC";
		}
		
		else if(eShuffleBytesMB + eTempObj <= eNGCMX) { //NGC can contain all the objects
			if(xms == 0) { //-Xms isn't set
				enOU = mergeBuffer;
				exOU = inMemSegBuffer;
				eNewU = eShuffleBytesMB + eTempObj;
				ssReason = "xms = 0, eEdenC < eShuffleBytesMB + eTempObj <= eNGCMX";
			}
			else { //EdenMX <= eShuffleBytesMB + eTempObj <= NGCMX
				enOU = 0;
				exOU = mergeBuffer;
				eNewU = eShuffleBytesMB + eTempObj;
				ssReason = "xms = xmx, eEdenMX <= eShuffleBytesMB + eTempObj <= eNGCMX";
			}
		}
		
		else { //eShuffleBytesMB + eTempObj > eNGCMX
			//if(eOGCMX < mergeBuffer) {
			//	System.err.println("eOGCMX is too small! eOGCMX = " + eOGCMX + ", mergeBuffer = " + mergeBuffer);
			//}
			
			if(xms == 0) {
				if(inMemSegBuffer < eOGC) {
					enOU = mergeBuffer; // maybe enOU = Math.min(eShuffleBytesMB, inMemSegBuffer);
					
				    exOU = inMemSegBuffer;
					eNewU = eNGCMX;
					ssReason = "eShuffleBytesMB + eTempObj > eNGCMX, xms = 0, inMemSegBuffer < eOGC";
				}
				else { //inMemSegBuffer >= eOGC
					enOU = Math.min(eOGC, mergeBuffer);
					if(eShuffleBytesMB / eOGCMX >= 2)
						exOU = eOGCMX;
					else
						exOU = Math.min(inMemSegBuffer, eOGCMX);
					eNewU = eNGCMX;
					ssReason = "eShuffleBytesMB + eTempObj > eNGCMX, xms = 0, inMemSegBuffer >= eOGC";
				}
			}
			
			else {
				if(inMemSegBuffer < eOGCMX) {
					enOU = inMemSegBuffer; // need more consideration
					exOU = eOGCMX;
					eNewU = eNGCMX;
					ssReason = "eShuffleBytesMB + eTempObj > eNGCMX, xms = xmx, inMemSegBuffer < eOGC";
				}
				
				else {
					enOU = mergeBuffer;
					exOU = eOGCMX;
					eNewU = eNGCMX;
					ssReason = "eShuffleBytesMB + eTempObj > eNGCMX, xms = xmx, inMemSegBuffer >= eOGC";
				}
			}
		}
		
			
		enOU = Math.min(eShuffleBytesMB, enOU + eFix);
		exOU = Math.min(eShuffleBytesMB, exOU + eFix); 
		
		// estimate the Perm Generation
		float ePermUsed = fSSPhaseMax.getPermUsed();	
		
		ReducerEstimatedJvmCost jvmCost = new ReducerEstimatedJvmCost();
		jvmCost.setSSPermUsed(ePermUsed);
		jvmCost.setSSNewUsed(eNewU);
		jvmCost.setSSnOU(enOU);
		jvmCost.setSSxOU(exOU);
		jvmCost.setSSnHeapU(enOU + eNewU);
		jvmCost.setSSxHeapU(exOU + eNewU);
		jvmCost.setSSTempObj(eTempObj);
		jvmCost.setSSFix(eFix);
		jvmCost.setSSReason(ssReason);
		
		jvmCost.seteShuffleBytesMB(eShuffleBytesMB);
		jvmCost.setMergeBuffer(mergeBuffer);
		jvmCost.setInMemSegBuffer(inMemSegBuffer);
		
		return jvmCost;
		
	}

	private float computeTempObj(List<JstatItem> subList, JvmModel fPhaseMax, Reducer fReducer) {
		float tempObj = 0;
		float shuffleBytesMB = (float) ((double)fReducer.getReducerCounters().getReduce_shuffle_raw_bytes() / (1024 * 1024));
		
		JstatItem lastJstat = subList.get(subList.size() - 1);
		if(lastJstat.getFGC() == 0 && lastJstat.getYGC() == 0) 
			tempObj = lastJstat.getEU() - shuffleBytesMB;
			
		else {
			float ascend = 0;
			float descend = 0;
			int size = subList.size();
			
			float[] c = new float[size];
			
			for(int i = 0; i < size; i++) {
				JstatItem item = subList.get(i);
				c[i] = item.getEU() + item.getS0U() + item.getS1U() + item.getOU();
			}
			
			ascend += c[0];
			for(int i = 1; i < c.length; i++) {
				if(c[i] > c[i-1])
					ascend += c[i] - c[i-1];  // contains InMemorySegments and accompanying temp objects 
				
				else if(c[i] < c[i-1]) {
					descend += (c[i-1] - c[i]); // revoke InMemorySegments(merged to disk) and temp objects
				}
			}
			
			if(ascend > shuffleBytesMB)
				tempObj = ascend - shuffleBytesMB;  // conservative
			else
				tempObj = descend;			// conservative
			
		}
					
		return tempObj;
	}

}
