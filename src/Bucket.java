import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class Bucket {

	private int lower_b;
	private int upper_b;
	private HashMap<Integer, Object> samples;
	private int cnt;
	
	public Bucket(int lb, int ub){
		this.cnt = 0;
		this.lower_b = lb;
		this.upper_b = ub;
		this.samples = new HashMap<Integer,Object>();
	}
	
	public Bucket(List<Object> samples){
		this.cnt = 0;
		this.samples = new HashMap<Integer,Object>();
		for(Object s:samples)
			if(s != null)
				this.samples.put(new Integer(cnt++), s);
	}
	
	public int getLowerB(){
		return lower_b;
	}
	
	public int getUpperB(){
		return upper_b;
	}
	
	public void insertGDP(Object s){
		if(s instanceof State)
			samples.put(new Integer(cnt++), s);
		else if(s instanceof HIT)
			samples.put(new Integer(cnt++), s);
	}
	
	public Collection<Object> getSamples(){
		return samples.values();
	}
}
