import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class Estimator {
	
	private int n; //sample size
	private int c; //unique classes in sample
	private int C; //total number of unique classes
	private int[] f;
	private HashMap<String,HistBar> hist; 
	private int n_sum;
	private int c_sum;
	private int f1_sum, f1_cnt;
	private int f12_sum, f12_cnt;
	
	class HistBar {
		private int lower_b;
		private int upper_b;
		private int count;
		
		HistBar(int lb, int ub, int count){
			this.lower_b = lb;
			this.upper_b = ub;
			this.count = count;
		}
		
		int getCount(){
			return count;
		}
		
		void setCount(int c){
			count = c;
		}
		
		double getLowerB(){
			return lower_b;
		}
		
		double getUpperB(){
			return upper_b;
		}
		
		void setLowerB(int lb){
			if(lower_b > lb)
				lower_b = lb;
		}
		
		void setUpperB(int ub){
			if(upper_b < ub)
				upper_b = ub;
		}
	}
	
	public Estimator(Object[] sample){
		n_sum = 0; 
		c_sum = 0;
		f1_sum = 0; 
		f12_sum = 0;
		hist = new HashMap<String,HistBar>();
		
		for(Object s:sample){
			if(s==null){ //why do we have bad samples?
				//System.out.println("Bad sample");
				continue;
			}
			if(s instanceof State){
				int gdp = ((State) s).getGDP();
				n_sum += gdp;
				
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
				int gdp = ((HIT) s).getGDP();
				n_sum += gdp;
				
				String k = ""+((HIT) s).getState();
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
		}
		this.n = sample.length;
		
		int[] f = new int[sample.length];
		Collection<HistBar> col = hist.values();
		Iterator<HistBar> itr = col.iterator();
		while(itr.hasNext()){
			HistBar bar = itr.next();
			int c = bar.getCount();
			double gdp = bar.getLowerB();
			if(c == 1){
				f1_sum += gdp;
				f12_sum += gdp;
				f1_cnt++;
				f12_cnt++;
			}
			else if(c == 2){
				f12_sum += gdp;
				f12_cnt++;
			}
			
			f[c-1] = f[c-1]+1;
		}
		this.f = f;
		this.c = col.size();
	}
	
	public double chao92(){
		if(n == 0)
			return 0;
		
		double cv = 0.0;
		double cov = 0.0;
		
		//compute coverage (good Turing Estimator)
		cov = 1 - (double) f[0]/n; //f[0] = f_1
		//compute CV
		int sum = 0;
		for(int i=0;i<c;i++){
			sum += i*(i+1)*f[i];
		}
		cv= Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0);
		
		if(cv == 0 || cov == 0)
			return chao84();
		
		return (double) c/cov + n*(1-cov)/cov*cv;
	}
	
	public double chao84(){
		if(n < 2 || f[1] == 0)
			return c;
		
		return c+f[0]*f[0]/2/f[1];
	}
	
	public double sumf1(){
		if(f1_cnt==0)
			return c_sum;
		return (double) c_sum + f1_sum/(double) f1_cnt * (chao92() - c);
	}
	
	public double sumf12(){
		if(f12_cnt==0)
			return c_sum;
		return (double) c_sum  + f12_sum/(double) f12_cnt * (chao92() - c);
	}
	
	public double sum(){
		if(c == 0) //(n==0)
			return c_sum;
		//System.out.println("c:"+c+" c_sum:"+c_sum);
		return (double) c_sum * chao92()/(double) c; //chao92 estimates the richness of species
		//return (double) n_sum * chao92()/(double) n; //chao92 estimates the population size
	}
}
