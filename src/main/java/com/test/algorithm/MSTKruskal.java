package com.test.algorithm;

import java.util.ArrayList;
import java.util.Scanner;

public class MSTKruskal {
	private static int MAX = 100;
	private ArrayList<Edge> edge = new ArrayList<Edge>();// 整个图的边
	private ArrayList<Edge> target = new ArrayList<Edge>();// 目标边，最小生成树
	private int[] parent = new int[MAX];// 标志所在的集合
	private static double INFINITY = 99999999.99;// 定义无穷大
	private double mincost = 0.0;// 最小成本
	private int n;// 结点个数

	public MSTKruskal() {
	}

	public static void main(String args[]) {
		MSTKruskal sp = new MSTKruskal();
		sp.init();
		System.out.println(sp.kruskal());
		sp.print();
	}

	// 初始化
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
			parent[i] = i;// 将每个顶点初始化为一个集合，父节点指向自己。
		}
	}

	// 集合合并，将父节点为j的改为父节点为k(父节点同为k的合并到一个集合。)
	public void union(int j, int k) {
		for (int i = 1; i <= n; ++i) {
			if (parent[i] == j) {
				parent[i] = k;
			}
		}
	}

	// prim算法主体
	public double kruskal() {
		// 找剩下的n-2条边
		int i = 0;
		while (i < n - 1 && edge.size() > 0) {
			// 每次取一最小边，并删除
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
			// 去掉环
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

	// 打印结果
	public void print() {
		for (int i = 0; i < target.size(); ++i) {
			Edge e = target.get(i);
			System.out.println("the " + (i + 1) + "th edge:" + e.start + "---"
					+ e.end);
		}
	}
}

class Edge {
	public int start;// 始边
	public int end;// 终边
	public double cost;// 权重
}
