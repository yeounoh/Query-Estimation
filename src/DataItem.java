public class DataItem {
	private String name;
	private int rank;
	private double val;
	private String[] ID;
	private long timestamp;
	
	/**
	 * 
	 * @param ID: array of at least two IDs: id[0] = sourceID, id[1] = (local) recordID.
	 * @param timestamp
	 * @param name
	 * @param val
	 * @param type: user-defined type of record (e.g., use for ranking)
	 */
	public DataItem(String[] ID, long timestamp, String name, double val, int rank){
		this.name = name;
		this.val = val;
		this.ID = ID;
		this.timestamp = timestamp;
		this.rank = rank;
	}
	
	public int rank(){
		return rank;
	}
	
	public String name(){
		return name;
	}
	
	public double value(){
		return val;
	}
	
	public String sourceID(){
		return ID[0];
	}
	
	public String recordID(){
		return ID[1];
	}
	
	public String[] ID(){
		return ID;
	}
	
	public long timestamp(){
		return timestamp;
	}
}
