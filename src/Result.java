
public class Result {
	double[] cnt;
	double[] sum;
	double[] measure; 
	double[] other;
	
	public Result(double[] cnt, double[] sum, double[] measure, double[] other){
		this.cnt = cnt;
		this.sum = sum;
		this.measure = measure;
		this.other = other;
	}
	
	public double[] summary(){
		double[] summary = new double[cnt.length + sum.length + measure.length + other.length]; 
		int i= 0;
		for(double c : cnt)
			summary[i++] = c;
		for(double s : sum)
			summary[i++] = s;
		for(double m : measure)
			summary[i++] = m;
		for(double m : other)
			summary[i++] = m;
		return summary;
	}
	
	public String toString(){
		String str = "";
		for(double c : cnt)
			str += c + " ";
		for(double s : sum)
			str += s + " ";
		for(double m : measure)
			str += m + " ";
		for(double m : other)
			str += m + " ";
		return str;
	}
}
