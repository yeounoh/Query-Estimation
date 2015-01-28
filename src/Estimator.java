import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class Estimator {
	
	private int n; //sample size
	private int c; //unique classes in sample
	//private int C; //total number of unique classes
	private int[] f;
	private HashMap<Integer, Object> samples;
	private HashMap<String,HistBar> hist; 
	private double c_sum;
	private double f1_sum, f12_sum;
	private double min, max;
		
	public Estimator(Object[] sample){
		c_sum = 0;
		f1_sum = 0;
		f12_sum = 0;
		min = Double.MAX_VALUE; 
		max = Double.MIN_VALUE;
		hist = new HashMap<String,HistBar>();
		samples = new HashMap<Integer,Object>();
		
		for(Object s:sample){
			if(s!=null){ //why do we have bad samples?
				samples.put(new Integer(n++), s);
				
				if(s instanceof State){
					double gdp = ((State) s).getGDP();
					
					if(gdp < min)
						min = gdp;
					if(gdp > max)
						max = gdp;
					
					String k = ""+((State) s).getName(); //key attribute is used for f-statistics
					if(!hist.containsKey(k)){
						hist.put(k, new HistBar(gdp, gdp, 1));
						c_sum += gdp;
					}
					else{ 
						HistBar bar = hist.get(k);
						bar.setCount(bar.getCount() + 1);
						bar.setLowerB(gdp);
						bar.setUpperB(gdp);
						hist.put(k, bar);
					}
				}
				else if(s instanceof HIT){
					double val = ((HIT) s).getValue();
					
					if(val < min)
						min = val;
					if(val > max)
						max = val;
					
					String k = ""+((HIT) s).getName();
					if(!hist.containsKey(k)){
						hist.put(k, new HistBar(val, val, 1));
						c_sum += val;
					}
					else{ 
						HistBar bar = hist.get(k);
						bar.setCount(bar.getCount() + 1);
						bar.setLowerB(val);
						bar.setUpperB(val);
						hist.put(k, bar);
					}
				}
			}
		}
		
		int[] f = new int[samples.size()];
		Collection<HistBar> col = hist.values();
		Iterator<HistBar> itr = col.iterator();
		while(itr.hasNext()){
			HistBar bar = itr.next();
			int cnt = bar.getCount();
			double val = bar.getLowerB();
			if(cnt == 1){
				f1_sum += val;
				f12_sum += val;
			}
			else if(cnt == 2){
				f12_sum += val;
			}
			
			f[cnt-1] = f[cnt-1]+1;
		}
		this.f = f;
		this.c = hist.size();
	}
	
	public double chao92(){
		if(n == 0)
			return 0;
		
		double cov = 1 - (double) f[0]/n; //f[0] = f_1
		
		double cv = 0.0;
		if(cov == 0)
			return c; //cv = Math.sqrt(n);
		else if((n-1) == 0)
			cv = Math.sqrt(n);
		else{
			int sum = 0;
			for(int i=0;i<c;i++)
				sum += i*(i+1)*f[i];
			
			cv = Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0);
		}
				
		return (double) c/cov + n*(1-cov)/cov*cv;
	}
	
	public double chao84(){
		if(n == 0)
			return 0;
		
		if(f[1] == 0)
			return c;
		
		return c+f[0]*f[0]/2/f[1];
	}
	
	//if f-1 is the best indicator of the missing classes, and we have very skewed value distribution, then 
	// value correction based on the statistics of the f-1 statistics might be better.
	public double sumf1(){
		if(f[0]==0)
			return c_sum;
		
		return (double) c_sum + f1_sum/(double) f[0] * (chao92() - c);
	}
	
	public double sumf12(){
		if((f[0]+f[1])==0)
			return c_sum;
		
		return (double) c_sum  + f12_sum/(double) (f[0]+f[1]) * (chao92() - c);
	}
	
	public double sum(){
		if(c == 0) //(n==0)
			return c_sum;
		
		return (double) c_sum * chao92()/(double) c; //chao92 estimates the richness of species
		//return (double) n_sum * chao92()/(double) n; //chao92 estimates the population size
	}

	public double sampleCov() {
		if(n == 0)
			return 0;
		
		return 1 - (double) f[0]/n; //f[0] = f_1, f1_cnt?	
	}
	
	public double coeffVar(){
		if(n == 0)
			return 0;
		
		double cov = 1 - (double) f[0]/n; //f[0] = f_1
		
		double cv = 0.0;
		if(cov == 0 || (n-1) == 0)
			cv = Math.sqrt(n);
		else{
			int sum = 0;
			for(int i=0;i<c;i++)
				sum += i*(i+1)*f[i]; //because i is zero-indexed
			
			cv = Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0); //System.out.println(""+cv+" "+((double) c/cov*((double) sum/(double) n/(double) (n-1))-1));
		}
		
		return cv;
	}

	/**
	 * Find out the optimal ER-bucket number -tackle each bucket separately.
	 * 1) split more to reduce CV
	 * 2) having too small C will overshoot
	 * 
	 * distributing samples to many buckets would help -> but, if there are too few samples in each bucket, we would overestimate.
	 * 
	 * @param threshold: minimum average sample coverage
	 * @param sample: validation data samples
	 * @return
	 */
	public Bucket[] autoBuckets(double threshold, ArrayList<Object> sample) {
		
		Bucket[] buckets = {new Bucket(min, max)};
		double avg = sampleCov();
		double sum = sum();
		
		int nbucket = 1;
		while(avg > threshold){
			//System.out.println(""+ nbucket + " " + avg + " " + sum);
			
			nbucket++;
			double range = (max - min)/nbucket;
			buckets = new Bucket[nbucket];
			for(int i=0;i<nbucket;i++){
				buckets[i] = new Bucket(min+i*range, min+(i+1)*range);
			}
			
			for(Object s:sample){
				if(s==null)
					continue;
				if(s instanceof HIT){
					double value = ((HIT) s).getValue();
					for(Bucket b : buckets){
						if(b.getLowerB() <= value && b.getUpperB() >= value){
							b.insertSample(s);
							break;
						}
					}
				}
				if(s instanceof State){
					double value = ((State) s).getGDP();
					for(Bucket b : buckets){
						if(b.getLowerB() <= value && b.getUpperB() >= value){
							b.insertSample(s);
							break;
						}
					}
				}
			}
			
			avg = 0; sum= 0;
			for(Bucket b : buckets){
				//avg += b.getSampleCov()/nbucket;
				avg += b.getSampleCov()*((double) b.getCount()/(double) n); 
				
				Object[] samples_b = b.getSamples().toArray();
				Estimator est = new Estimator(samples_b);
				sum += est.sum();
				//System.out.print(" "+(double) b.getCount()/(double) n + ":"+b.getSampleCov());
			}
		}
		
		return buckets;
	}
}
