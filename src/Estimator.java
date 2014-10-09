import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

public class Estimator {
	
	private int n; //sample size
	private int c; //unique classes in sample
	private int C; //total number of unique classes
	private int[] f;
	private int t_sum;
	
	class HistBar {
		int range_a;
		int range_b;
		int count;
		
		HistBar(int range_a, int range_b, int count){
			this.range_a = range_a;
			this.range_b = range_b;
			this.count = count;
		}
	}
	
	public Estimator(Person[] sample){
		t_sum = 0;
		HashMap<String,HistBar> hist = new HashMap<String,HistBar>();
		for(Person s:sample){
			if(s==null){ //why do we have bad samples?
				//System.out.println("Bad sample");
				continue;
			}
			
			t_sum += s.getCoffee();
			
			String k = ""+s.getID();
			if(!hist.containsKey(k)){
				hist.put(k, new HistBar(s.getID(),s.getID(),1));
			}
			else{
				HistBar bar = hist.get(k);
				bar.count++;
				hist.put(k,bar);
			}
		}
		this.n = sample.length;
		
		int[] f = new int[sample.length];
		Collection<HistBar> col = hist.values();
		Iterator<HistBar> itr = col.iterator();
		while(itr.hasNext()){
			HistBar bar = itr.next();
			f[bar.count-1] = f[bar.count-1]+1;
		}
		this.f = f;
		this.c = col.size();
	}
	
	
	public double chao92(){
		double cv= 0.0;
		double cov = 0.0;
		
		//compute coverage (good Turing Estimator)
		cov = 1 - (double) f[0]/n; //f[0] = f_1
		//compute CV
		int sum = 0;
		for(int i=0;i<c;i++){
			sum += i*(i+1)*f[i];
		}
		cv= Math.max((double) c/cov*((double) sum/(double) n/(double) (n-1))-1, 0);
		
		if(n==0)
			return 0;
		return (double) c/cov + n*(1-cov)/cov*cv;
	}
	
	public double chao84(){
		if(n==0)
			return 0;
		return c+f[0]*f[0]/2/f[1];
	}
	
	public double sum(){
		if(n==0)
			return 0;
		return (double) t_sum * chao92()/(double) n;
	}
}
