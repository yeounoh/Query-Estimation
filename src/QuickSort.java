
public class QuickSort {

	int partition(Object arr[], int left, int right){
		int i= left, j= right;
		Object tmp; 
		double pivot = ((DataItem) arr[(left + right) / 2]).value();
		
		while(i <= j){
			while(((DataItem) arr[i]).value() < pivot)
				i++;
			while(((DataItem) arr[j]).value() > pivot)
				j--; 
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
