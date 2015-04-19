import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class Estimator {
	
	private int n; //sample size
	private int c; //unique classes in sample
	//private int C; //total number of unique classes
	private int[] f;
	private HashMap<Integer, Double[]> dist;
	private HashMap<Integer, Object> samples;
	//private HashMap<String,HistBar> hist; 
	private double c_sum;
	private double f1_sum, f12_sum;
	private double min, max;
		
	public Estimator(Object[] sample){
		c_sum = 0;
		f1_sum = 0;
		f12_sum = 0;
		min = Double.MAX_VALUE; 
		max = Double.MIN_VALUE;
		//hist = new HashMap<String,HistBar>();
		dist = new HashMap<Integer, Double[]>();
		samples = new HashMap<Integer,Object>();
		
		for(Object s:sample){
			if(s!=null){ //why do we have bad samples?
				samples.put(new Integer(n++), s);
				
				double val = ((DataItem) s).value();
				
				if(val < min)
					min = val;
				if(val > max)
					max = val;
					
				Integer k = new Integer(((DataItem) s).rank());
				if(dist.containsKey(k)){
					Double[] v = dist.get(k);
					dist.replace(k, new Double[]{v[0].doubleValue()+1, v[1]});
				}
				else{
					c_sum += val;
					dist.put(k, new Double[]{new Double(1), val});
				}
			}
		}
		
		int[] f = new int[samples.size()];
		Iterator<Integer> itr = dist.keySet().iterator();
		while(itr.hasNext()){
			Integer k = itr.next();
			Double[] v = dist.get(k);
			double cnt = v[0].doubleValue();
			double val = v[1].doubleValue();
			
			if(cnt == 1){
				f1_sum += val;
				f12_sum += val;
			}
			else if(cnt == 2){
				f12_sum += val;
			}
			
			f[(int)cnt-1] = f[(int)cnt-1]+1;
			
			dist.replace(k, new Double[]{v[0].doubleValue(), v[1]});
		}
		this.f = f;
		this.c = dist.size(); 
	}
	
	public int getUniqueCount() {
		return c;
	}
	
	public double getSampleMean(){
		double mean = 0.0;
		for(Object s : samples.values().toArray()){
			mean += ((DataItem) s).value();
		}
		return mean/n;
	}
	
	public double getSampleStd(){
		double mean = 0.0, sqrd_mean = 0.0;
		for(Object s : samples.values().toArray()){
			double v = ((DataItem)s).value();
			mean += v;
			sqrd_mean += v*v;
		}
		mean = mean/n;
		sqrd_mean = sqrd_mean/n;
		
		return Math.sqrt(sqrd_mean - mean*mean);
	}
	
	public double getMax(){
		return max;
	}
	
	public double getMin(){
		return min;
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
		
		if(f.length < 2 || f[1] == 0)
			return c;
		
		return c+f[0]*f[0]/2/f[1];
	}
	
	// # samples for workers: n_w[i], where i indexes worker.
	public double MonteCarlo(double C_hat, int[] n_w, int err_type){
		int n_sim = 100;
		Random r = new Random();
		int n_size = 0;
		for(int i : n_w)
			n_size += i;
		
		double err_sum = 0;
		for(int rep=0;rep<n_sim;rep++){
			
			HashMap<Integer,Double> simulated = new HashMap();
			for(int i=0;i<n_w.length;i++){ //for each worker
				HashMap<Integer,Double> simulated_w = new HashMap(); //without replacement
				int j= 0;
				while(j<Math.min(C_hat,n_w[i])){
					int rank = (int) Math.floor(r.nextDouble()*C_hat) + 1; //rank instead of value
					if(!simulated_w.containsKey(new Integer(rank))){
						simulated_w.put(new Integer(rank), new Double(1));
						j++;
						if(!simulated.containsKey(new Integer(rank)))
							simulated.put(new Integer(rank), new Double(1));
						else
							simulated.replace(new Integer(rank), simulated.get(new Integer(rank)).doubleValue()+1);
					}
				}
			}
			double f1_sim = 0, f2_sim = 0;
			Iterator<Integer> keys = simulated.keySet().iterator();
			while(keys.hasNext()){
				Integer k = keys.next();
				double cnt = simulated.get(k).doubleValue();
				if(cnt == 1){
					f1_sim++;
				}
				else if(cnt == 2){
					f2_sim++;
				}
			} 
			
			if(err_type == 1){
				if(f2_sim != 0)
					err_sum += Math.abs(chao84() - (simulated.keySet().size() + f1_sim*f1_sim/2/f2_sim));
			}
			else if(err_type == 2){ //kullback-leibler divergence
				Set<Integer> Pkeys = dist.keySet();
				Set<Integer> Qkeys = simulated.keySet();
				
				Set<Integer> PQkeys = new HashSet<Integer>();
				PQkeys.addAll(Pkeys);
				PQkeys.addAll(Qkeys);
				Iterator<Integer> PQkeys_itr = PQkeys.iterator();
				double kl = 0; //laplace smoothing taken into account
				while(PQkeys_itr.hasNext()){
					Integer k = PQkeys_itr.next();
					double q= 0, p= 0;
					if(dist.containsKey(k))
						p = (dist.get(k)[0].doubleValue()+1)/(samples.size()+PQkeys.size());
					else
						p = 1.0/(samples.size()+PQkeys.size());
					
					if(simulated.containsKey(k))
						q = (simulated.get(k).doubleValue()+1)/(n_size+PQkeys.size());
					else
						q = 1.0/(n_size+PQkeys.size());
					
					if(p != 0) //this is not possible with laplace smoothig, but note that 0*ln0 -> 0
						kl += p*Math.log(p/q);
				}
				err_sum += kl;
			}
			else if(err_type == 3){ // kendal tau distance
				
			}
			else
				err_sum = 0;
		}
		return err_sum/n_sim;
	}
	
	//if f-1 is the best indicator of the missing classes, and we have very skewed value distribution, then 
	// value correction based on the statistics of the f-1 statistics might be better.
	public double sumf1(){ 
		if(c == 0 || f[0] == 0)
			return c_sum;
		
		return (double) c_sum + f1_sum/(double) f[0] * (chao92() - c);
	}
	
	public double sumf12(){
		if(c == 0 || (f[0]+f[1]) == 0)
			return c_sum;
		
		return (double) c_sum + f12_sum/c*(chao92()-c);
	}
	
	public double csum(){
		return c_sum;
	}
	
	public double sumEst(int C_hat){
		if(c == 0)
			return c_sum;
		
		return (double) c_sum + f1_sum/c*(C_hat-c);
	}
	
	public double sumEst(){
		if(c == 0) //(n==0)
			return c_sum;
		
		return (double) c_sum + c_sum/c*(chao92()-c); //chao92 estimates the richness of species
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
		
		return Math.sqrt(cv);
	}

	/**
	 * Find out the optimal ER-bucket number -tackle each bucket separately.
	 * 1) split more to reduce CV
	 * 2) having too small C will overshoot? Having too few samples per bucket will overestimate
	 * 
	 * distributing samples to many buckets would help -> but, if there are too few samples in each bucket, we would overestimate.
	 * 
	 * In this method, we always split a bucket in halves.
	 * 
	 * @param thresh_c: minimum sample coverage (obsolete)
	 * @param thresh_cv: minimum coefficient of variance 
	 * @param sample: validation data samples
	 * @return
	 */
	public Bucket[] autoBuckets(double thresh_cv, Object[] sample) {
		int nbkt_prev = 0;
		
		ArrayList<Bucket> buckets = new ArrayList<Bucket>(); 
		Bucket init_b = new Bucket(min,max);
		
		new QuickSort().quickSort(sample,0,sample.length-1);
		for(Object s : sample){
			init_b.insertSample(s);
		} 
		buckets.add(init_b);
		
		while(buckets.size() > nbkt_prev){
			/**
			//To monitor splitting processes
			String desc = "";
			for(Bucket b : buckets){
				//desc += "["+ b.getLowerB() + "," + b.getUpperB()+"]"+b.getCoeffVar()+", "+b.getCount();
				desc += "["+ b.getLowerB() + "," + b.getUpperB()+"] "+b.getCount();
			}
			System.out.println(desc);
			*/
			nbkt_prev = buckets.size(); 
			ArrayList<Bucket> toAdd = new ArrayList<Bucket>();
			Iterator<Bucket> itr = buckets.iterator();
			while(itr.hasNext()){ 
				Bucket b = itr.next();
				
				if(b.getCount() == 0){
					itr.remove();
					continue;
				}
				
				//split the bucket if sample coverage is higher than the threshold
				if(b.getUnique() > 3 && b.getCoeffVar() > thresh_cv){
					Object[] samples_b = b.getSamples().toArray();
					
					//sort the samples
					new QuickSort().quickSort(samples_b,0,samples_b.length-1);
					
					double lb = b.getLowerB();
					double ub = b.getUpperB();
					double split = 0.0;
					ArrayList<Bucket> buckets_b = new ArrayList<Bucket>();
					
					split = (lb + ub)/2;
					buckets_b.add(new Bucket(lb, split));
					buckets_b.add(new Bucket(split, ub));
					
					for(int i=0;i<samples_b.length;i++){
						Object s = samples_b[i];
						double v = ((DataItem) s).value();
						if(v >= lb && v <= split)
							buckets_b.get(0).insertSample(s);
						else{
							buckets_b.get(1).insertSample(s);
						}
					}
					
					double prev = buckets_b.get(0).sumEst() + buckets_b.get(1).sumEst();
					for(Object ss : samples_b){
						split = ((DataItem)ss).value();
						Bucket lbkt = new Bucket(lb,split);
						Bucket rbkt = new Bucket(split,ub);
						
						for(int i=0;i<samples_b.length;i++){
							Object s = samples_b[i];
							double v = ((DataItem)s).value();
							if(v >= lb && v <= split)
								lbkt.insertSample(s);
							else
								rbkt.insertSample(s);
						}
						
						if(prev > lbkt.sumEst() + rbkt.sumEst()){
							buckets_b.clear();
							buckets_b.add(lbkt);
							buckets_b.add(rbkt);
							prev = lbkt.sumEst() + rbkt.sumEst();
						}
					}
					
					itr.remove();
					toAdd.addAll(buckets_b);
				}
			}
			
			Iterator<Bucket> itr_add = toAdd.iterator();
			while(itr_add.hasNext()){ 
				Bucket b = itr_add.next();
				
				if(b.getCount() == 0){
					itr_add.remove();
				}
			}
			buckets.addAll(toAdd);
		}
		
		Bucket[] output = new Bucket[buckets.size()];
		return buckets.toArray(output);
	}
}
