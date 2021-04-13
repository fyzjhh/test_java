package com.test.algorithm.other;

public class ZuHe {
	
	public void combine(int[] a, int n) {
		
		if(null == a || a.length == 0 || n <= 0 || n > a.length)
			return;
			
		int[] b = new int[n];
		getCombination(a, n , 0, b, 0);
	}

	private void getCombination(int[] a, int n, int begin, int[] b, int index) {
		
		if(n == 0){//�����n�����ˣ����b����
			for(int i = 0; i < index; i++){
				System.out.print(b[i] + " ");
			}
			System.out.println();
			return;
		}
			
		for(int i = begin; i < a.length; i++){
			
			b[index] = a[i];
			getCombination(a, n-1, i+1, b, index+1);
		}
		
	}
	
	public static void main(String[] args){
		
		ZuHe zuHe = new ZuHe();
		
		int[] a = {1,2,3,4};
		for (int i = 1; i <= a.length; i++) {
			zuHe.combine(a,i);
		}

		

	}

}
