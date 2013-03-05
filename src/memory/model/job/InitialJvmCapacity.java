package memory.model.job;

import java.util.List;

import profile.commons.configuration.Configuration;
import profile.commons.metrics.GcCapacity;
import profile.task.mapper.Mapper;
import profile.task.reducer.Reducer;



public class InitialJvmCapacity {

	//-----finished task-----
	private int fXmx;
	private int fXms;
	private float fNGCMX;
	private float fOGCMX;
	private float fPGCMX;
	
	private float fNGC;
	private float fOGC;
	private float fPGC;
	
	private float fS0C;
	private float fS1C;
	private float fEC;
	
	//-----new task-----
	private int eXmx;
	private int eXms;
	private float eNGCMX;
	private float eOGCMX;
	private float ePGCMX;
	
	private float eNGC;
	private float eOGC;
	private float ePGC;
	
	private float eS0C;
	private float eS1C;
	private float eEC;
	
	private String error = "";

	
	public InitialJvmCapacity(Configuration fConf, Configuration newConf, 
			List<Mapper> mapperList, List<Reducer> reducerList) {
		fXmx = fConf.getXmx();
		if(fXmx < 300) {
			System.err.println("The -Xmx heap size is too small!");
			//System.exit(1);
		}
		fXms = getXms(fConf);
		GcCapacity gc = mapperList.get(0).getMetrics().getGcCapacity();
		initFinishedGcCapacity(gc);
		
		eXmx = newConf.getXmx();
		eXms = getXms(newConf);
		initNewGcCapacity(newConf);
	}
	
	public InitialJvmCapacity(Configuration fConf, GcCapacity gcCap) {
		fXmx = fConf.getXmx();
		if(fXmx < 300) {
			System.err.println("The -Xmx heap size is too small!");
			//System.exit(1);
		}
		fXms = getXms(fConf);
		
		initFinishedGcCapacity(gcCap);
		
		eXmx = fConf.getXmx();
		eXms = getXms(fConf);
		initNewGcCapacity(fConf);
	}
	
	private void initFinishedGcCapacity(GcCapacity gc) {
		fNGCMX = gc.getNGCMX();
		fOGCMX = gc.getOGCMX();
		fPGCMX = gc.getPGCMX();
		
		//NGCMN    NGCMX     NGC     S0C   S1C       EC      OGCMN      OGCMX       OGC         OC      PGCMN    PGCMX     PGC       PC     YGC    FGC 
		//85504.0 1365312.0  85504.0 10688.0 10688.0  64128.0   171136.0  2730688.0   171136.0   171136.0  21248.0  83968.0  21248.0  21248.0      0     0
		if(fXms == 0) {
			fNGC = 85504f / 1024;
			fOGC = 171136f / 1024;
			fPGC = 21248f / 1024;
			
			fS0C = 10688f / 1024;
			fS1C = 10688f / 1024;
			fEC = 64128f / 1024;
			
			fXms = (int) (fNGC + fOGC);
		}
		// OGC = 2 * NGC, EC = 6 * SC ==> NGC = Xms / 3, OGC = 2 * Xms / 3, S0C = NGC / 8
		else {
			fNGC = fXms / 3;
			fOGC = fXms - fNGC;
			fPGC = 21248f / 1024;
			
			fS0C = fNGC / 8;
			fS1C = fS0C;
			fEC = fNGC - fS0C - fS1C;
		}

	}

	private void initNewGcCapacity(Configuration conf) {
		
		ePGCMX = 83968f / 1024;
		eNGCMX = (float)eXmx / 3;
		eOGCMX = 2 * eNGCMX;
		
		if(eOGCMX < conf.getIo_sort_mb()) {
			//System.err.println("OutOfMemory will occur!");
			error = "OutOfMemory [OGCMX = " + String.format("%1$-3.1f", eOGCMX) + "]";
		}
		
		if(eXms == 0) {
			eNGC = 85504f / 1024;
			eOGC = 171136f / 1024;
			ePGC = 21248f / 1024;
			
			eS0C = 10688f / 1024;
			eS1C = 10688f / 1024;
			eEC = 64128f / 1024;
			
			eXms = (int) (eNGC + eOGC);
		}
		// OGC = 2 * NGC, EC = 6 * SC ==> NGC = Xms / 3, OGC = 2 * Xms / 3, S0C = NGC / 8
		else {
			eNGC = (float)eXms / 3;
			eOGC = (float)eXms - eNGC;
			ePGC = 21248f / 1024;
			
			eS0C = eNGC / 8;
			eS1C = eS0C;
			eEC = eNGC - eS0C - eS1C;
		}
	}

	private int getXms(Configuration conf) {
		String jvmParam = conf.getConf("mapred.child.java.opts");
		if(jvmParam.contains("-Xms")) {
			int start = jvmParam.indexOf("-Xms") + 4;
			int end = jvmParam.indexOf('m', start);
			return Integer.parseInt(jvmParam.substring(start, end)); //4000
		}
		else
			return 0;
	}

	public int getfXmx() {
		return fXmx;
	}

	public int getfXms() {
		return fXms;
	}

	public float getfNGCMX() {
		return fNGCMX;
	}

	public float getfOGCMX() {
		return fOGCMX;
	}

	public float getfPGCMX() {
		return fPGCMX;
	}

	public float getfNGC() {
		return fNGC;
	}

	public float getfOGC() {
		return fOGC;
	}

	public float getfPGC() {
		return fPGC;
	}

	public float getfS0C() {
		return fS0C;
	}

	public float getfS1C() {
		return fS1C;
	}

	public float getfEC() {
		return fEC;
	}

	public int geteXmx() {
		return eXmx;
	}

	public int geteXms() {
		return eXms;
	}

	public float geteNGCMX() {
		return eNGCMX;
	}

	public float geteOGCMX() {
		return eOGCMX;
	}

	public float getePGCMX() {
		return ePGCMX;
	}

	public float geteNGC() {
		return eNGC;
	}

	public float geteOGC() {
		return eOGC;
	}

	public float getePGC() {
		return ePGC;
	}

	public float geteS0C() {
		return eS0C;
	}

	public float geteS1C() {
		return eS1C;
	}

	public float geteEC() {
		return eEC;
	}
	
	public String getError() {
		return error;
	}

}
