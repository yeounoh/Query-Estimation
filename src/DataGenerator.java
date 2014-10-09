import java.sql.SQLException;
import java.util.Random;


public class DataGenerator {
	
	/**
	 * 
	 * @param db
	 * @param table
	 * @param p_size
	 * @param base base for id (e.g. starts from 0 or 100?)
	 * @param dist_type 1- uniform, 2- Gaussian
	 * @throws SQLException
	 */
	public void generateData(Database db, String table, int p_size, int base) throws SQLException {
		Person[] people = generatePeople(p_size, base);
		for(int i=0;i<people.length;i++){
			db.insert(table, people[i]);
		}
	}
	
	public Person[] generatePeople(int n, int base){	
		Person[] people = new Person[n];
		Random r = new Random();
		
		for(int i=0;i<n;i++){
			//coffee: normally distributed over a 0 to 9 continuous range
			int nc = Math.max(0, (int) Math.floor(r.nextGaussian()*(r.nextInt(2)+0.5)+r.nextInt(3)+1));
			people[i] = new Person(base+(i+1), r.nextInt(30)+10, nc);
		}
		
		return people;
	}
}
