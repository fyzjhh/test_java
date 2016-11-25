package com.test.algorithm;

public class ZuHe {
	
	public void combine(int[] a, int n) {
		
		if(null == a || a.length == 0 || n <= 0 || n > a.length)
			return;
			
		int[] b = new int[n];//辅助空间，保存待输出组合数
		getCombination(a, n , 0, b, 0);
	}

	private void getCombination(int[] a, int n, int begin, int[] b, int index) {
		
		if(n == 0){//如果够n个数了，输出b数组
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
		
		ZuHe robot = new ZuHe();
		
		int[] a = {1,2,3,4};
		for (int i = 1; i <= a.length; i++) {
			robot.combine(a,i);	
		}

		

	}

}
