import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.math3.distribution.PoissonDistribution;

public class Estimator {
	
	private int n; //sample size
	private int c; //unique classes in sample
	//private int C; //total number of unique classes
	private int[] f;
	private HashMap<Integer, double[]> dist;
	private HashMap<Integer, Object> samples;
	//private HashMap<String,HistBar> hist; 
	private double c_sum;
	private double f1_sum, f12_sum;
	private double min, max;
	
	private int specific_item_cnt;
	
	// For incidence-based approaches
	private int[] Q;
	private double q1_sum, q12_sum;
	
	// For Abundance Coverage Estimator (ACE)
	private int c_rare; //the number of rare species in a sample (each with 10 or fewer individuals)
	private int c_abun; //the number of abundant species in a sample (each with more than 10 individuals)
	private int n_rare; //the total number of individuals in the rare species
		
	public Estimator(Object[] sample){
		specific_item_cnt = 0;
		
		c_sum = 0;
		f1_sum = 0;
		f12_sum = 0;
		min = Double.MAX_VALUE; 
		max = Double.MIN_VALUE;
		//hist = new HashMap<String,HistBar>();
		dist = new HashMap<Integer, double[]>(); //Double[]{count, value}
		samples = new HashMap<Integer,Object>();
		
		for(Object s:sample){
			if(s!=null){ //why do we have bad samples?
				samples.put(new Integer(n++), s);
				
				double val = ((DataItem) s).value();
				
				if(val < min)
					min = val;
				if(val > max)
					max = val;
					
				Integer k = new Integer(((DataItem) s).rank()); //rank is like a unique ID
				if(dist.containsKey(k)){
					double[] v = dist.get(k);
					v[0] += 1; 
					v[2] += 1; // because each worker/source is sampling without replacement
					dist.replace(k, v);
				}
				else{
					c_sum += val;
					dist.put(k, new double[]{1, val, 1}); // (abundance, value, incidence)
				}
				
				if(((DataItem) s).rank() == 100){ // smallest value among 100 data items
					specific_item_cnt++; //specific item count
				}
			}
		}
		
		int[] f = new int[samples.size()];
		int n = 0;
		int n_rare = 0;
		int c_rare = 0;
		int c_abun = 0;
		Iterator<Integer> itr = dist.keySet().iterator();
		while(itr.hasNext()){
			Integer k = itr.next();
			double[] v = dist.get(k);
			double cnt = v[0];
			double val = v[1];
			
			if(cnt == 1){
				f1_sum += val;
				f12_sum += val;
			}
			else if(cnt == 2){
				f12_sum += val;
			}
			
			f[(int)cnt-1] = f[(int)cnt-1]+1;
			n += cnt;
			
			if(cnt <= 10) {
				c_rare++;
				n_rare += cnt;
			}
			else c_abun++;
		}
		this.f = f;
		this.c = dist.size(); 
		this.n = n;
		
		this.c_rare = c_rare;
		this.c_abun = c_abun;
		this.n_rare = n_rare;
	}
	
	public int getF1Count(){
		return f[0];
	}
	
	public int getSpecificItemCnt(){
		return specific_item_cnt;
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
	
	public double getRankMean(){
		double mean = 0.0;
		for(Object s : samples.values().toArray()){
			mean += ((DataItem) s).rank();
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
	
	// Abundance Coverage Estimator
	public double ACE(){
		if(n == 0)
			return 0;
		
		double cov_ACE = 1 - (double) f[0]/n_rare;
		
		double cv = 0.0;
		if(cov_ACE == 0) 
			return chao92(); //f1=n_rare, return Chao1
		else if((n-1) == 0)
			cv = Math.sqrt(n);
		else{
			int sum = 0;
			for(int i=0;i<10;i++)
				sum += i*(i+1)*f[i];
			
			cv = Math.max((double) c_rare/cov_ACE*((double) sum/(double) n_rare/(double) (n_rare-1))-1, 0);
		}
				
		return Math.max((double) c_abun + c_rare/cov_ACE + f[0]*cv/cov_ACE, c);
	}
	
	// Valiant's estimator
	public double unseen(){
		if(n == 0) return 0;
		
		double grid_factor = 1.05; // x_i (the grid of prob) will be geometric with this ratio
		double alpha = 0.5; // avoid overfitting, smaller value increase the risk
		int max_itr = 1000;
		
		// minimum allowable probability
		double xLP_min = 1.0/(n*Math.max(10,n));
		int i_min = 0;
		for(int i=0;i<f.length; i++) if(f[i]>0) i_min = i;
		if(i_min > 0) xLP_min = (double) (i_min+1.0)/n;
		
		// split the f-statistics into the dense portion and the sparse portion
		ArrayList<Double> x = new ArrayList<Double>();
		ArrayList<Double> histx = new ArrayList<Double>();
		ArrayList<Double> fLP = new ArrayList<Double>();
		for (int i=0;i<f.length;i++){
			if (f[i] > 0){
				int i_lower = (int) Math.max(0, i-Math.ceil(Math.sqrt(i)));
				int i_upper = (int) Math.min(f.length-1, i+Math.ceil(Math.sqrt(i)));
				int sum = 0;
				for(int j=i_lower;j<=i_upper;j++) sum += f[j];
				if(sum < Math.sqrt(i)){
					// sparse region used the empirical histogram
					x.add((double) (i+1.0)/n);
					histx.add((double) f[i]);
					fLP.add(0.0);
				}
				else{
					// wil use LP for dense region
					fLP.add((double) f[i]);
				}
			}
		}
		
		// no LP portion
		double fLPsum = 0;
		for(double fp : fLP) fLPsum += fp;
		if (fLPsum == 0.0){
			x.remove(0);
			histx.remove(0);
			double histxsum = 0.0;
			Iterator<Double> histxitr = histx.iterator();
			while(histxitr.hasNext()){
				histxsum += histxitr.next().doubleValue();
			}
			return histxsum;
		}
		
		// first LP problem
		double xhistxsum = 0.0;
		for(int i=0;i<x.size();i++) xhistxsum += x.get(i) * histx.get(i);
		double LP_mass = 1 - xhistxsum;
		double f_max = 0.0;
		int idxMax = 0;
		for(int i=0;i<fLP.size();i++){
			if(f_max < fLP.get(i)){
				f_max = fLP.get(i);
				idxMax = i;
			}
		}
		f_max = f.length - 1 - (fLP.size()-1-idxMax);
		List<Double> fLP1 = fLP.subList(0, (int) f_max+1);
		List<Double> fLP2 = Arrays.asList(new Double[(int)Math.ceil(Math.sqrt(f_max))]);
		fLP = new ArrayList<Double>(fLP1);
		fLP.addAll(fLP2);
		int szLPf = fLP.size();
		
		double xLP_max = (f_max+1)/(double) n;
		int szLPx = (int) Math.ceil(Math.log(xLP_max/xLP_min)/Math.log(grid_factor)) +1;
		double[] xLP = new double[szLPx];
		for(int i=0;i<szLPx;i++) xLP[i] = xLP_min * Math.pow(grid_factor, i);
		
		double[] objf = new double[szLPx + 2*szLPf]; 
		for(int i=szLPx;i<objf.length;i+=2)
			objf[i] = 1.0/Math.sqrt(fLP.get((i-szLPx)/2)+1);
		for(int i=szLPx+1;i<objf.length;i+=2)
			objf[i] = 1.0/Math.sqrt(fLP.get((i-szLPx-1)/2)+1);
		
		double[][] A = new double[2*szLPf][szLPx+2*szLPf];
		double[] b = new double[2*szLPf];
		for(int i=0;i<szLPf;i++){
			for(int j=0;j<szLPx;j++){
				PoissonDistribution poisson = new PoissonDistribution(n*xLP[j]);
				A[2*i][j] = poisson.probability(i+1);
				A[2*i+1][j] = -1 * A[2*i][j];
			}
			A[2*i][szLPx+2*i] = -1;
			A[2*i+1][szLPx+2*i+1] = -1;
			b[2*i] = fLP.get(i);
			b[2*i+1] = fLP.get(i);
		}
		double[] Aeq = new double[szLPx+2*szLPf];
		for(int i=0;i<szLPx;i++) Aeq[i] = xLP[i];
		double beq = LP_mass;
		
		for(int i=0;i<szLPx;i++){
			for(int j=0;j<2*szLPf;j++)
				A[j][i] = A[j][i]/xLP[i];
			Aeq[i] = Aeq[i]/xLP[i];
		}
		
		//results consists of x, slack, success, status, nit, message
		
		
		return 0.0;
	}
	
	public double chao92(){
		if(n == 0)
			return 0;
		
		double cov = 1 - (double) f[0]/n; //f[0] = f_1
		
		double cv = 0.0;
		if(cov == 0) 
			return Double.POSITIVE_INFINITY;
			//return c; //when there is no overlaps, we can't estimate
		else if((n-1) == 0)
			cv = Math.sqrt(n);
		else{
			int sum = 0;
			for(int i=0;i<c;i++)
				sum += i*(i+1)*f[i];
			
			cv = Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0);
		}
				
		return Math.max((double) c/cov + n*(1-cov)/cov*cv, c);
	}
		
	public double chao84(){
		if(n == 0)
			return 0;
		
		if(f.length < 2 || f[1] == 0)
			return c;
		
		return Math.max(c+f[0]*f[0]/2/f[1], c);
	}
	
	// First-order jackknife estimator
	public double jackknife1(){
		if(n == 0)
			return 0;
		return (double) c + f[0];
	}
	
	// Second-order jackknife estimator
	public double jackknife2(){
		if(n == 0) return 0;
		if(f.length < 2) return c;
		
		return (double) c + 2*f[0] - f[1];
	}
	
	// # samples for workers: n_w[i], where i indexes worker.
	public double MonteCarlo(int C_hat, int[] n_w, double lambda, boolean ideal){
		int n_sim = 500; //simulation number
		Random r = new Random();
		int n_size = 0;
		for(int i : n_w)
			n_size += i;
		//rank items (by frequency)
		//HashMap<Integer, double[]> dist_ranked = ranking(dist);
		
		//double[] f_sim = new double[n_size];
		double err_sum = 0;
		for(int rep=0;rep<n_sim;rep++){
			
			HashMap<Integer,double[]> simulated = new HashMap();
			for(int i=0;i<n_w.length;i++){ //for each worker
				HashMap<Integer,double[]> simulated_w = new HashMap(); //without replacement
				int j= 0;
				while(j<Math.min(C_hat,n_w[i])){
					int rank = ((int) Math.floor(r.nextDouble()*(C_hat+1))); //rank instead of value
					//double pdf = (double) 1/C_hat; //uniform dist
					double pdf = lambda < 0 ? 1-Math.exp((rank)*lambda/C_hat) : Math.exp(-1*(rank)*lambda/C_hat);
					
					if(r.nextDouble() <= pdf && !simulated_w.containsKey(new Integer(rank))){
						simulated_w.put(new Integer(rank), new double[]{1,0});
						j++;
						if(!simulated.containsKey(new Integer(rank)))
							simulated.put(new Integer(rank), new double[]{1,0});
						else{
							double[] v = simulated.get(rank);
							v[0] += 1;
							simulated.replace(new Integer(rank), v);
						}
					}
				}
			}
			//int[] f_sim = new int[n_size];
//			Iterator<Integer> itr = simulated.keySet().iterator();
//			while(itr.hasNext()){
//				Integer k = itr.next();
//				double[] v = simulated.get(k);
//				double cnt = v[0];
//				double val = v[1];
//				
//				f_sim[(int)cnt-1] = f_sim[(int)cnt-1]+1/n_size;
//			}
			
			//kullback-leibler divergence
			//the indexes of P and Q don't have the same meaning; 
			//they just indicate that each key is an unique item within each set;
			//but, consecutive keys are important to compare the shapes of p and q distributions
			
			//distribution & indexing based
			Set<Integer> Qkeys = simulated.keySet();
			HashMap<Integer, double[]> dist_ranked = ideal? dist : ranking(Qkeys);
			Set<Integer> Pkeys = dist_ranked.keySet();
			
			Set<Integer> PQkeys = new HashSet<Integer>();
			PQkeys.addAll(Pkeys);
			PQkeys.addAll(Qkeys);
			Iterator<Integer> PQkeys_itr = PQkeys.iterator();
			double kl = 0; //laplace smoothing taken into account
			while(PQkeys_itr.hasNext()){
				Integer k = PQkeys_itr.next();
				double q= 0, p= 0;
				if(dist_ranked.containsKey(k))
					p = (dist_ranked.get(k)[0]+0.1)/(samples.size()+(double) PQkeys.size()*0.1);
				else
					p = 0.1/(samples.size()+(double) PQkeys.size()*0.1);
				
				if(simulated.containsKey(k))
					q = (simulated.get(k)[0]+0.1)/(n_size+(double) PQkeys.size()*0.1);
				else
					q = 0.1/(n_size+(double) PQkeys.size()*0.1);

//				if(dist_ranked.containsKey(k))
//					p = (dist_ranked.get(k)[0])/(samples.size());
//				if(simulated.containsKey(k))
//					q = (simulated.get(k)[0])/(n_size);

				if(p != 0 && q != 0) //this is not possible with laplace smoothig, but note that 0*ln0 -> 0
					kl += p*Math.log(p/q);
			}
			err_sum += kl;
		}
//		double kl = 0; //laplace smoothing taken into account
//		for(int i=0;i<f.length;i++){
//			double q= 0, p= 0;
//			if(f[i] > 0)
//				p = ((i+1)*f[i]+0.1)/(n_size*(1+0.1));
//			else
//				p = 0.1/(n_size*(1+0.1));
//
//			if(f_sim[i]>0)
//				q = ((i+1)*f_sim[i]+0.1)/(n_size*(1+0.1)); //(n_size+PQkeys.size());
//			else
//				q = 0.1/(n_size*(1+0.1)); //PQkeys.size());
//
//			kl += p*Math.log(p/q);
//			kl += Math.abs(f[i] - f_sim[i])*1/Math.sqrt(1+f[i]);
//		}
//		err_sum += kl;
//		return err_sum;
		return err_sum/n_sim;
	}
	
	//ranking by frequency
	private HashMap<Integer, double[]> ranking(HashMap<Integer, double[]> dist){
		HashMap<Integer, double[]> dist_ranked = new HashMap<Integer, double[]>();
		
		Object[] sorted = dist.values().toArray(); //double[] : (freq,value)
		new QuickSort().quickSort(sorted,0,sorted.length-1); //sort by frequency
		
		for(int i=0;i<sorted.length;i++){
			double[] s_ = (double[]) sorted[i];
			//System.out.print(s_[0] + " ");
			dist_ranked.put(new Integer(i+1), s_);
		}
		//System.out.println();
		
		return dist_ranked;
	}
	
	//ranking by the simulated ranks
	private HashMap<Integer, double[]> ranking(Set<Integer> ranks){
		
		HashMap<Integer, double[]> dist_ranked = new HashMap<Integer, double[]>();
		Object[] sample = samples.values().toArray();
		
		Iterator<Integer> itr = ranks.iterator();
		int rank = itr.next();
		
		new QuickSort().quickSort(sample,0,sample.length-1);
		
		double prev_value = ((DataItem) sample[sample.length-1]).value();
		for(int i=sample.length-1;i>=0;i--){
			int prev_rank = ((DataItem) sample[i]).rank();
			double cur_value = ((DataItem) sample[i]).value();
			
			if(cur_value < prev_value){
				prev_value = cur_value;
				if(itr.hasNext()){
					rank = itr.next();
				}
				else{
					rank++;
				}
			}
			
			double[] cnt_value = dist.get(prev_rank);
			dist_ranked.put(rank, cnt_value);
		}
		return dist_ranked;
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
		
		return (double) c_sum + f12_sum/(f[0] + f[1])*(chao92()-c);
	}
	
	public double csum(){
		return c_sum;
	}
	
	public double sumEst(int C_hat){
		if(c == 0)
			return c_sum;
		
		return (double) c_sum + c_sum/c*(C_hat-c);
	}
	
	public double sumf1Est(int C_hat){
		if(c == 0 || f[0] == 0)
			return c_sum;
		
		return (double) c_sum + f1_sum/(double) f[0] * (C_hat - c);
	}
	
	public double sumEst(){
		if(c == 0) //(n==0)
			return c_sum;
		
		return (double) c_sum + c_sum/c*(chao92()-c); 
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

	public double countEstUpperBound(){
		double delta = 0.01; //at least with probability 1-delta
		double bound = (2*Math.sqrt(2)+Math.sqrt(3))*Math.sqrt(Math.log(3/delta)/n);
		return c/(1-Math.min((sampleCov()-1)*-1+bound,1));
	}
	
	public double sumEstUpperBound(){
		double delta = 0.01; //at least with probability 1-delta 99.85
		double bound = (2*Math.sqrt(2)+Math.sqrt(3))*Math.sqrt(Math.log(3/delta)/n);
		
		int C_hat = (int) Math.ceil(c/(1-Math.min((sampleCov()-1)*-1+bound,1)));

		double v_est = c_sum/c;
		double std = 0.0;
		Iterator<Integer> itr = dist.keySet().iterator();
		while(itr.hasNext()){
			Integer k = itr.next();
			double[] v = dist.get(k);
			double cnt = v[0];
			double val = v[1];
			
			std += (val-v_est)*(val-v_est)/(c-1);
		}
		std = Math.sqrt(std);
		
		double s_est = c_sum + (c_sum/c + 3*std)*(C_hat-c);
		
		return s_est;
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
	public Bucket[] autoBuckets(double thresh_cv, Object[] sample, boolean ub_test) {
		int nbkt_prev = 0;
		
		ArrayList<Bucket> buckets = new ArrayList<Bucket>(); 
		Bucket init_b = new Bucket(min,max);
		
		new QuickSort().quickSort(sample,0,sample.length-1);
		for(Object s : sample){
			init_b.insertSample(s);
		} 
		buckets.add(init_b);
		
		while(buckets.size() > nbkt_prev){
			nbkt_prev = buckets.size(); 
			ArrayList<Bucket> toAdd = new ArrayList<Bucket>();
			Iterator<Bucket> itr = buckets.iterator();
			while(itr.hasNext()){ 
				Bucket b = itr.next();
				
				if(b.getCount() == 0){
					itr.remove();
					continue;
				}
				
				//Paul's bound
//				double d1 = Math.sqrt(b.getF1Count())/b.getUnique();
//				double d2 = 1/Math.sqrt(b.getCount()-b.getF1Count());
//				double condition = Math.sqrt((d1/b.getUnique())*(d1/b.getUnique()) + (d2/(b.getCount()-b.getF1Count()))*(d2/(b.getCount()-b.getF1Count())));
//				if(condition < 0.01){
				
				//value estimation guidance
//				if(b.getUnique() > 3 && b.getCoeffVar() > thresh_cv){
				
				//simple condition
				if(b.getF1Count() < b.getCount() && b.getIdx() != -1){
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
					
					double prev = ub_test? buckets_b.get(0).sumEstUpperBound() 
							+ buckets_b.get(1).sumEstUpperBound()
							: buckets_b.get(0).sumEst() + buckets_b.get(1).sumEst();
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
						
						if(ub_test){
							double new_ub = lbkt.sumEstUpperBound() + rbkt.sumEstUpperBound();
							if(prev > new_ub){
								buckets_b.clear();
								buckets_b.add(lbkt);
								buckets_b.add(rbkt);
								prev = new_ub;
							}
						}
						else{
							double new_sum = lbkt.sumEst() + rbkt.sumEst();
							if(prev > new_sum){
								buckets_b.clear();
								buckets_b.add(lbkt);
								buckets_b.add(rbkt);
								prev = new_sum;
							}
						}
					}
										
					itr.remove();
					if(ub_test)
						if(prev < b.sumEstUpperBound())
							toAdd.addAll(buckets_b);
						else{
							b.setIdx(-1);
							toAdd.add(b);
						}
					else
						if(prev < b.sumEst())
							toAdd.addAll(buckets_b);
						else{
							b.setIdx(-1);
							toAdd.add(b);
						}
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
		
		Bucket[] output = buckets.toArray(new Bucket[buckets.size()]);
		new QuickSort().quickSort(output,0,output.length-1);
		
		return output;
	}
}
