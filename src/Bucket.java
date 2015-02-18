import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Bucket {

	private double lower_b;
	private double upper_b;
	private HashMap<Integer, Object> samples;
	private HashMap<String, HistBar> hist;
	private int[] f;
	private int n;
	private int c;
	private int idx;
	
	public Bucket(double lb, double ub){
		this.n = 0;
		this.c = 0;
		this.f = null;
		this.lower_b = lb;
		this.upper_b = ub;
		this.samples = new HashMap<Integer,Object>();
		this.hist = new HashMap<String,HistBar>();
	}
	
	public Bucket(double lb, double ub, Object[] sample){
		this.n = 0;
		this.lower_b = lb;
		this.upper_b = ub;
		this.samples = new HashMap<Integer,Object>();
		this.hist = new HashMap<String,HistBar>();
	}
	
	public Bucket(Object[] sample){
		this.n = 0;
		this.samples = new HashMap<Integer,Object>();
		this.hist = new HashMap<String,HistBar>();
		
		for(Object s:sample){
			if(s != null){
				this.samples.put(new Integer(n++), s);
				
				double v = ((DataItem) s).value();
				String k = ((DataItem) s).name();
				if(!hist.containsKey(k)){
					hist.put(k, new HistBar(v, v, 1));
				}
				else{ 
					HistBar bar = hist.get(k);
					bar.setCount(bar.getCount() + 1);
					bar.setLowerB(v);
					bar.setUpperB(v);
					hist.put(k, bar);
				}
			}
		}
		
		int[] f = new int[this.samples.size()];
		Collection<HistBar> col = hist.values();
		Iterator<HistBar> itr = col.iterator();
		while(itr.hasNext()){
			HistBar bar = itr.next();
			int cnt = bar.getCount();
			f[cnt-1] = f[cnt-1]+1;
		}
		this.f = f;
		c = hist.size();
	}
	
	/**
	 * int[] f must have been initialized via Bucket(object[] sample) call? no
	 * @param s: a sample
	 */
	public void insertSample(Object s){
		if(s != null){
			samples.put(new Integer(n++), s); 
			
			double v = ((DataItem) s).value();
			String k = ((DataItem) s).name(); 
			if(!hist.containsKey(k)){
				hist.put(k, new HistBar(v, v, 1));
			}
			else{ 
				HistBar bar = hist.get(k);
				bar.setCount(bar.getCount() + 1);
				bar.setLowerB(v);
				bar.setUpperB(v);
				hist.put(k, bar);
			}
			
			int[] f = new int[this.samples.size()];
			Collection<HistBar> col = hist.values();
			Iterator<HistBar> itr = col.iterator();
			while(itr.hasNext()){
				HistBar bar = itr.next();
				int cnt = bar.getCount();
				f[cnt-1] = f[cnt-1]+1;
			}
			this.f = f;
			c = hist.size(); 
		}
	}
	
	public void setIdx(int i){
		this.idx = i;
	}
	
	public int getIdx(){
		return idx;
	}
	
	public double getSampleCov(){
		Estimator est = new Estimator(samples.values().toArray());
		return est.sampleCov();
	}
	
	public double getCoeffVar(){
		Estimator est = new Estimator(samples.values().toArray());
		return est.coeffVar();
	}
	
	public int getCount(){
		return n;
	}
	
	public int getUnique(){
		return c;
	}
	
	public int getF1Count(){
		return f[0];
	}
	
	public double getLowerB(){
		return lower_b;
	}
	
	public double getUpperB(){
		return upper_b;
	}
	
	public double sum(){
		Estimator est = new Estimator(samples.values().toArray());
		return est.sum();
	}
	
	public Collection<Object> getSamples(){
		return samples.values();
	}
	
	public String toString(){
		return "["+lower_b+", "+upper_b+"]";
	}
}
