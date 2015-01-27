
public class State {
	private String name; //unique id
	private int rank; //rank by gdp, can be used as a unique id (~#states)
	private double gdp;
	
	public State(String name, int rank, double gdp){
		this.name = name;
		this.rank = rank;
		this.gdp = gdp;
	}
	
	public String getName(){
		return name;
	}
	
	public int getRank(){
		return rank;
	}
	
	public double getGDP(){
		return gdp;
	}
}