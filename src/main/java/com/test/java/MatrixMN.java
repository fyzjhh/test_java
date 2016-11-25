package com.test.java;

public class MatrixMN {
	public static void main(String[] args) throws Exception {
		int row = 5;
		int col = 4;

		int[][] array = new int[row][col];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				array[i][j] = (int) (Math.random() * 10) + 1;
			}
		}

		printMatrix(array);

		System.out.println(test(array,row,col));
	}

	public static int test(int[][] array, int row, int col) {

		int[][] matrix = new int[row+1][col+1];

		// ��һ��
		for (int i = 0; i <= col; i++) {
			matrix[0][i] = 0;
		}
		// ��һ��
		for (int i = 0; i <= row; i++) {
			matrix[i][0] = 0;
		}

		printMatrix(matrix);
		
		for (int i = 1; i <= row; i++) {
			for (int j = 1; j <= col; j++) {
				int x = array[i-1][j-1]; // ��ǰλ�õ�ֵ
				
				int left = matrix[i][j - 1]; // ��ߵ�
				int up = matrix[i - 1][j];// �����

				matrix[i][j] = Math.max(x+left , x+up);

			}

		}

		// Printing the matrix
		printMatrix(matrix);

		return matrix[row][col];
	}

	private static void printMatrix(int[][] matrix) {
		for (int[] row_val : matrix) {
			for (int val : row_val) {
				System.out.format("%5d", val);
			}
			System.out.println();
		}
		System.out.println();
	}
}
