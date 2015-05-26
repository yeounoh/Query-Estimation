
public class QuickSort {

	int partition(Object arr[], int left, int right){
		int i= left, j= right;
		Object tmp;
		double pivot = 0;
		if(arr[0] instanceof DataItem)
			pivot = ((DataItem) arr[(left + right) / 2]).value();
		else if(arr[0] instanceof Bucket)
			pivot = ((Bucket) arr[(left + right) / 2]).getLowerB();
		else if(arr[0] instanceof double[])
			pivot = ((double[]) arr[(left + right) / 2])[0];
		
		while(i <= j){
			if(arr[0] instanceof DataItem){
				while(((DataItem) arr[i]).value() < pivot)
					i++;
				while(((DataItem) arr[j]).value() > pivot)
					j--; 
			}
			else if(arr[0] instanceof Bucket){
				while(((Bucket) arr[i]).getLowerB() < pivot)
					i++;
				while(((Bucket) arr[j]).getLowerB() > pivot)
					j--; 
			}
			else if(arr[0] instanceof double[]){
				while(((double[]) arr[i])[0] < pivot)
					i++;
				while(((double[]) arr[j])[0] > pivot)
					j--;
			}
			
			if(i <= j){
				tmp = arr[i];
				arr[i] = arr[j];
				arr[j] = tmp;
				i++;
				j--;
			}
		}
		return i;
	}
	
	void quickSort(Object arr[], int left, int right){
		int index = partition(arr, left, right);
		if(left < index - 1)
			quickSort(arr, left, index-1);
		if(index < right)
			quickSort(arr, index, right);
	}
	
}
