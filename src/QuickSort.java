
public class QuickSort {

	int partition(Object arr[], int left, int right){
		int i= left, j= right;
		Object tmp;
		if(arr[0] instanceof State){
			int pivot = ((State) arr[(left + right) / 2]).getGDP();
			
			while(i <= j){
				while(((State) arr[i]).getGDP() < pivot)
					i++;
				while(((State) arr[j]).getGDP() > pivot)
					j--;
				if(i<=j){
					tmp = arr[i];
					arr[i] = arr[j];
					arr[j] = tmp;
					i++;
					j--;
				}
			}
		}
		else if(arr[0] instanceof HIT){
			int pivot = ((HIT) arr[(left + right) / 2]).getGDP();
			
			while(i <= j){
				while(((HIT) arr[i]).getGDP() < pivot)
					i++;
				while(((HIT) arr[j]).getGDP() > pivot)
					j--; 
				if(i<=j){
					tmp = arr[i];
					arr[i] = arr[j];
					arr[j] = tmp;
					i++;
					j--;
				}
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
