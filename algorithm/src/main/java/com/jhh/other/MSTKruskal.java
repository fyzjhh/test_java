package com.test.algorithm.other;

import java.util.ArrayList;
import java.util.Scanner;

public class MSTKruskal {
	private static int MAX = 100;
	private ArrayList<Edge> edge = new ArrayList<Edge>();// ����ͼ�ı�
	private ArrayList<Edge> target = new ArrayList<Edge>();// Ŀ��ߣ���С������
	private int[] parent = new int[MAX];// ��־���ڵļ���
	private static double INFINITY = 99999999.99;// ���������
	private double mincost = 0.0;// ��С�ɱ�
	private int n;// ������

	public MSTKruskal() {
	}

	public static void main(String args[]) {
		MSTKruskal sp = new MSTKruskal();
		sp.init();
		System.out.println(sp.kruskal());
		sp.print();
	}

	// ��ʼ��
	public void init() {
		Scanner scan = new Scanner(System.in);
		int p, q;
		double w;

		System.out.println("spanning tree begin!Input the node number:");
		n = scan.nextInt();
		System.out.println("Input the graph(-1,-1,-1 to exit)");

		while (true) {
			p = scan.nextInt();
			q = scan.nextInt();
			w = scan.nextDouble();
			if (p < 0 || q < 0 || w < 0) {
				break;
			}
			Edge e = new Edge();
			e.start = p;
			e.end = q;
			e.cost = w;
			edge.add(e);
		}

		mincost = 0.0;

		for (int i = 1; i <= n; ++i) {
			parent[i] = i;// ��ÿ�������ʼ��Ϊһ�����ϣ����ڵ�ָ���Լ���
		}
	}

	// ���Ϻϲ��������ڵ�Ϊj�ĸ�Ϊ���ڵ�Ϊk(���ڵ�ͬΪk�ĺϲ���һ�����ϡ�)
	public void union(int j, int k) {
		for (int i = 1; i <= n; ++i) {
			if (parent[i] == j) {
				parent[i] = k;
			}
		}
	}

	// prim�㷨����
	public double kruskal() {
		// ��ʣ�µ�n-2����
		int i = 0;
		while (i < n - 1 && edge.size() > 0) {
			// ÿ��ȡһ��С�ߣ���ɾ��
			double min = INFINITY;
			int tag = 0;
			Edge tmp = null;
			for (int j = 0; j < edge.size(); ++j) {
				Edge tt = edge.get(j);
				if (tt.cost < min) {
					min = tt.cost;
					tmp = tt;
				}
			}
			int jj = parent[tmp.start];
			int kk = parent[tmp.end];
			// ȥ����
			if (jj != kk) {
				++i;
				target.add(tmp);
				mincost += tmp.cost;
				union(jj, kk);
			}
			edge.remove(tmp);
		}
		if (i != n - 1) {
			System.out.println("no spanning tree");
			return 0;
		}
		return mincost;
	}

	// ��ӡ���
	public void print() {
		for (int i = 0; i < target.size(); ++i) {
			Edge e = target.get(i);
			System.out.println("the " + (i + 1) + "th edge:" + e.start + "---"
					+ e.end);
		}
	}
}

class Edge {
	public int start;// ʼ��
	public int end;// �ձ�
	public double cost;// Ȩ��
}
