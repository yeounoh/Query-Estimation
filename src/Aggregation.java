
/**
 * 
 * SELECT column_name, aggregate_function(column_name)
 * FROM table_name
 * WHERE column_name operator value
 * GROUP BY column_name;
 * 
 * @author user
 *
 */
public class Aggregation {

	
	public double avg(){
		
		return 0.0;
	}
	
	public double sum(Person[] people){
		int sum = 0;
		for(Person s:people){
			if(s == null)
				continue;
			sum += s.getCoffee();
		}
		return sum;
	}
	
	public int count(){
		return 0;
	}
	
	
}
