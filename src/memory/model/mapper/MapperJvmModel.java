package memory.model.mapper;

import java.util.ArrayList;
import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.JstatItem;
import profile.commons.metrics.JvmItem;
import profile.task.mapper.Mapper;

import memory.model.job.InitialJvmCapacity;
import memory.model.jvm.JvmMetricsUtil;
import memory.model.jvm.JvmModel;
import memory.model.jvm.MapperEstimatedJvmCost;


public class MapperJvmModel {
	private Configuration fConf;
	private Configuration newConf;
	private InitialJvmCapacity gcCap;
	
	private int fismb;
	private int fkvbuffer;
    private int fkvoffsets;
    private int fkvindices;
	
	public MapperJvmModel(Configuration fConf, Configuration newConf, InitialJvmCapacity gcCap) {
		this.fConf = fConf;
		this.newConf = newConf;
		this.gcCap = gcCap;
		computeBuffer(fConf);
	}

	private void computeBuffer(Configuration fConf) {
		float spillper = fConf.getIo_sort_spill_percent();
		float recper = fConf.getIo_sort_record_percent();
		int sortmb = fConf.getIo_sort_mb();

		// buffers and accounting
		long maxMemUsage = sortmb << 20;
		long recordCapacity = (long) (maxMemUsage * recper);
		recordCapacity -= recordCapacity % 16;
		long kvbufferBytes = maxMemUsage - recordCapacity;

		recordCapacity /= 16;
		long kvoffsetsLen = recordCapacity;
		long kvindicesLen = recordCapacity * 3;

		int mb = 1024 * 1024;
		this.fismb = fConf.getIo_sort_mb();
		this.fkvbuffer = (int) (kvbufferBytes / mb);
		this.fkvoffsets = (int) (kvoffsetsLen / mb);
		this.fkvindices = (int) (kvindicesLen / mb);	
	}


	//return average of (OU - fismb)
	private float computeFix(List<JstatItem> jstatItemList) {
		List<Float> fixList = new ArrayList<Float>();
		for(JstatItem item : jstatItemList) {
			if(item.getOU() > fismb) 
				fixList.add(item.getOU() - fismb);
		}
		
		if(fixList.isEmpty())
			return 0;
		else {
			float sum = 0;
			for(Float f : fixList) 
				sum += f;
			return sum / fixList.size();
		}
	}
	
	//for split.size not changed
	public MapperEstimatedJvmCost estimateJvmCost(Mapper fMapper, Mapper eMapper) {
		
		List<JstatItem> jstatItemList = fMapper.getMetrics().getJstatMetricsList();
		//Occasionally, jstatItemList is empty (unknown reason).
		//In this occasion, we just return null and ignore this fMapper
		if(jstatItemList.isEmpty())
			return null;
		List<JvmItem> jvmItemList = fMapper.getMetrics().getJvmMetricsList();
		
		JvmModel fMapPhaseMax = computeFinishedMaxJvmCost(jstatItemList, jvmItemList);
		
		//float fix = computeFix(jstatItemList); this algorithm may cause abnormal estimation
		// so we let user to set this value
		float fix = 0;
		
		float eTempObj = computeTempObj(jstatItemList, fMapPhaseMax, fConf.getXms());

		MapperEstimatedJvmCost jvmCost = estimateBasedOnRules(fMapPhaseMax, fix, eTempObj, false);
		
		return jvmCost;
	}

	

	

	private MapperEstimatedJvmCost estimateBasedOnRules(JvmModel fMapPhaseMax, float fix, float eTempObj, boolean splitChanged) {
		
		// estimate the Perm Generation
		float ePermUsed = fMapPhaseMax.getPermUsed();
		
		float ismb = newConf.getIo_sort_mb();
		float eOU;
		float eNGU;
		float eHeapU = 0;
		
		String reason;
		
		if(ismb + fix + eTempObj <= gcCap.geteEC()) {
			if(ismb >= fConf.getIo_sort_mb() && splitChanged == false && fMapPhaseMax.getOldUsed() > 0 
					&& gcCap.geteEC() <= gcCap.getfEC()) {
				eOU = ismb;
				eNGU = ismb + fix + eTempObj;
				eHeapU = eNGU;
				reason = "ismb >= fismb, fOU > 0, ismb + fix + eTempObj <= eEC";
			}
			else {
				eNGU = ismb + fix + eTempObj;
				eOU = 0;
				reason = "ismb + fix + eTempObj <= eEC";
			}
			
		}
		else {
			eOU = ismb + fix;
			float eNGCMX = gcCap.geteNGCMX();
			
			if(newConf.getXms() == 0 && fConf.getXms() == 0 && fMapPhaseMax.getEdenUsed() <= gcCap.getfEC()) {
				eNGU = fMapPhaseMax.getEdenUsed();
				reason = "eXms=0, fXms=0, fEU < fEC";
			}
				
			else {
				eNGU = Math.min(eNGCMX, eTempObj);
				reason = "eOU = ismb + fix, eNGU = min(eNGCMX, eTempObj)";
			}
				
			
		}	
		
		MapperEstimatedJvmCost jvmCost = new MapperEstimatedJvmCost();
		jvmCost.setPermUsed(ePermUsed);
		jvmCost.setNewUsed(eNGU);
		jvmCost.setOU(eOU);
		jvmCost.setTempObj(eTempObj);
		jvmCost.setFix(fix);
		if(eHeapU == 0)
			jvmCost.setHeapU(eNGU + eOU); 
		else
			jvmCost.setHeapU(eHeapU);
		jvmCost.setReason(reason);
		
		return jvmCost;
	}


	private float computeTempObj(List<JstatItem> jstatItemList, JvmModel fMapPhaseMax, int fXms) {
		float tempObj = 0;
		
		JstatItem lastJstat = jstatItemList.get(jstatItemList.size() - 1);
		if(lastJstat.getFGC() == 0 && lastJstat.getYGC() == 0) {
			// used to handle the abnormal Jstat bug in some occasion
			if(jstatItemList.get(0).getEU() >= fismb) 
				tempObj = lastJstat.getEU() - jstatItemList.get(0).getEU();
				
			else
				tempObj = fMapPhaseMax.getHeapUsed() - fismb;
			return tempObj;
		}
			
		// used to handle the abnormal Jstat bug in some occasion
		JstatItem firstJstat = jstatItemList.get(0);
		if(firstJstat.getYGC() > 0 && firstJstat.getEU() > fismb && lastJstat.getFGC() == 0 && lastJstat.getYGC() == 1) {
			if(jstatItemList.size() == 1)
				return 0;
			else
				return lastJstat.getEU() - jstatItemList.get(1).getEU();
		}
			

		
		float[] c = new float[jstatItemList.size()];
		
		for(int i = 0; i < jstatItemList.size(); i++) {
			JstatItem item = jstatItemList.get(i);
			c[i] = item.getEU() + item.getS0U() + item.getS1U() + item.getOU();
			if(c[i] > fismb)
				c[i] = c[i] - fismb;
		}
		
		int monitorGC = 0;
		
		tempObj += c[0];
		for(int i = 1; i < c.length; i++) {
			if(c[i] > c[i-1])
				tempObj += c[i] - c[i-1];
			
			else if(c[i] < c[i-1]) {
				//we assume all the fix is thrown into old gen
				if(jstatItemList.get(i).getOU() > fismb)
					tempObj += c[i] - (jstatItemList.get(i).getOU() - fismb);
				else
					tempObj += c[i];
				
				monitorGC++;
			}
				
		}
			
		
		
		//we need to enlarge tempObj when xms is too small with frequent GC
		if(fXms == 0) {
			int gcCount = jstatItemList.get(jstatItemList.size() - 1).getYGC()
					+ jstatItemList.get(jstatItemList.size() - 1).getFGC();
			if(monitorGC != 0)
			//need more consideration
				tempObj = tempObj * Math.min(2.0f, gcCount / monitorGC);
			else if(gcCount > 0) {
				tempObj += gcCap.getfEC() * gcCount;
			}
			
		}
		
		
		return tempObj;
	}

	private JvmModel computeFinishedMaxJvmCost(List<JstatItem> jstatItemList, List<JvmItem> jvmItemList) {
		//find the max value in map-first-spill phase and spill-finished phase
		JvmModel fMapPhaseMax = JvmMetricsUtil.maxJstatValue(jstatItemList, 0, jstatItemList.size() - 1);
				
		JvmMetricsUtil.updateMaxJvmValue(fMapPhaseMax, jvmItemList, 0, jvmItemList.size() - 1);
						
		return fMapPhaseMax;
	}

	//for split.size changed
	//this algorithm is not graceful, need more consideration
	public MapperEstimatedJvmCost estimateJvmCost(List<Mapper> fMapperList, Mapper eMapper) {

		List<MapperEstimatedJvmCost> tempJvmCostList = new ArrayList<MapperEstimatedJvmCost>();
		
		for(Mapper fMapper : fMapperList) {
			List<JstatItem> jstatItemList = fMapper.getMetrics().getJstatMetricsList();
			if(jstatItemList.isEmpty())
				continue;
			List<JvmItem> jvmItemList = fMapper.getMetrics().getJvmMetricsList();
			
			JvmModel fMapPhaseMax = computeFinishedMaxJvmCost(jstatItemList, jvmItemList);
			
			float fix = computeFix(jstatItemList);
			float eTempObj = computeTempObj(jstatItemList, fMapPhaseMax, fConf.getXms());
			
			eTempObj = scaleTempObj(fMapper, eMapper, eTempObj); //need more consideration
			
			MapperEstimatedJvmCost jvmCost = estimateBasedOnRules(fMapPhaseMax, fix, eTempObj, true);
			tempJvmCostList.add(jvmCost);
		}
		
		// select the mapper with max HeapU
		return selectMaxJvmCost(tempJvmCostList);
		
	}

	
	private MapperEstimatedJvmCost selectMaxJvmCost(List<MapperEstimatedJvmCost> tempJvmCostList) {
		int selectIndex = 0;
		float maxHeapUsed = 0;
		for(int i = 0; i < tempJvmCostList.size(); i++) {
			if(tempJvmCostList.get(i).getHeapU() > maxHeapUsed) {
				maxHeapUsed = tempJvmCostList.get(i).getHeapU();
				selectIndex = i;
			}
		}
		return tempJvmCostList.get(selectIndex);
	}


	private float scaleTempObj(Mapper fMapper, Mapper eMapper, float fTempObj) {
		long fMap_input_bytes = fMapper.getMapperCounters().getMap_input_bytes();
		long fMap_output_bytes = fMapper.getMapperCounters().getMap_output_bytes();
		
		long eMap_input_bytes = eMapper.getMapperCounters().getMap_input_bytes();
		long eMap_output_bytes = eMapper.getMapperCounters().getMap_output_bytes();
		
		double inputRatio = (double)eMap_input_bytes / fMap_input_bytes;
		double outputRatio = (double)eMap_output_bytes / fMap_output_bytes;
		
		double interpolation = inputRatio * 0.5 + outputRatio * 0.5;
		
		return (float) (fTempObj * interpolation);
	}


	/*
	//Heuristic Algorithm: find the max minimum value after youth or full GC
	private float estimateMinEdenUsed(List<JstatItem> jstatItemList) {	
		if(jstatItemList.size() <= 3) {
			float eden = 0;
			for(int i = 0; i < jstatItemList.size(); i++) {
				if(jstatItemList.get(i).getEU() > eden) {
					eden = jstatItemList.get(i).getEU();
				}
			}
			return eden;
		}
		
		float maxMinEden = 0; // max minimum value
		float maxMaxEden = 0; // max maximum value //to be considered
		for(int i = 1; i < jstatItemList.size() - 1; i++) {
			float currentEU = jstatItemList.get(i).getEU();
			if(currentEU < jstatItemList.get(i - 1).getEU() && currentEU <= jstatItemList.get(i + 1).getEU()) {
				if(currentEU > maxMinEden)
					maxMinEden = currentEU;
			}
			
			else if(currentEU > jstatItemList.get(i - 1).getEU() && currentEU > jstatItemList.get(i + 1).getEU()) {
				if(currentEU > maxMaxEden)
					maxMaxEden = currentEU;
			}
		}
		
		float startEden = jstatItemList.get(0).getEU();
		//to be considered
		float endEden = jstatItemList.get(jstatItemList.size() - 1).getEU();
		
		if(maxMinEden < startEden)
			maxMinEden = startEden;
		
		return maxMinEden;
	}
	
	
	//Heuristic Algorithm: find the max minimum value after youth or full GC
	private float estimateMaxEdenUsed(List<JstatItem> jstatItemList) {	
		float maxEden = jstatItemList.get(0).getEU();
		for(int i = 1; i < jstatItemList.size(); i++) {
			float cEU = jstatItemList.get(i).getEU();
			if(cEU >= jstatItemList.get(i - 1).getEU())
				maxEden += (cEU - jstatItemList.get(i - 1).getEU());
			else {
				//we don't know whether the left objects are all new-generated or left behind after GC.
				//So we assume half of the left objects are new-generated.
				maxEden += cEU / 2; 
			}
				
		}
		return maxEden;
	}
	*/
}
