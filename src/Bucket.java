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
	
	public void setIdx(int i){
		this.idx = i;
	}
	
	public int getIdx(){
		return idx;
	}
	
	public double getSampleCov(){
		if(n == 0)
			return 0;
		//System.out.println(f[0]);
		return 1-(double) f[0]/(double) n;
	}
	
	public double getCoeffVar(){
		if(n == 0)
			return 0;
		
		double cov = 1 - (double) f[0]/n; //f[0] = f_1
		
		double cv = 0.0;
		if(cov == 0 || (n-1) == 0)
			cv = Math.sqrt(n);
		else{
			int sum = 0;
			for(int i=0;i<c;i++)
				sum += i*(i+1)*f[i];
			
			cv = Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0);
		}
		
		return Math.sqrt(cv);
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
	
	public Collection<Object> getSamples(){
		return samples.values();
	}
}
