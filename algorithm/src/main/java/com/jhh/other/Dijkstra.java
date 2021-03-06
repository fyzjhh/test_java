package com.test.algorithm.other;

public class Dijkstra {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Graph theGraph = new Graph();

		theGraph.addVertex('A');// 0

		theGraph.addVertex('B');// 1

		theGraph.addVertex('C');// 2

		theGraph.addVertex('D');// 3

		theGraph.addVertex('E');// 4

		theGraph.addOneEdge(0, 1, 50);// AB 50

		theGraph.addOneEdge(0, 3, 80);// AD 80

		theGraph.addOneEdge(1, 2, 60);// BC 60

		theGraph.addOneEdge(1, 3, 90);// BD 90

		theGraph.addOneEdge(2, 4, 40);// CE 40

		theGraph.addOneEdge(3, 2, 20);// DC 20

		theGraph.addOneEdge(3, 4, 70);// DE 70

		theGraph.addOneEdge(4, 1, 50);// EF 50

		System.out.println("Dijkstra: ");

		theGraph.dijkstra();
	}

}

/*
 * 
 * Graph.java
 */

class Vertex {

	public char label;

	public boolean isInTree;

	public Vertex(char label) {

		this.label = label;

		isInTree = false;

	}

}

// sPath[]�����洢���ڵ�;��롣

class DistPare {

	public int parentVertex;

	public int distance;

	public DistPare(int parentVertex, int distance) {

		this.parentVertex = parentVertex;

		this.distance = distance;

	}

}

class Graph {

	private final int MAX_VERTEX = 20;

	private final int INFINITY = 999999;

	private int nVerts;

	private int nTree;

	private int currentVertex;

	private int startToCurrent;

	private int adjMatrix[][];

	private Vertex vertexList[];

	private DistPare sPath[];

	public Graph() {

		adjMatrix = new int[MAX_VERTEX][MAX_VERTEX];

		vertexList = new Vertex[MAX_VERTEX];

		sPath = new DistPare[MAX_VERTEX];

		nVerts = 0;

		nTree = 0;

		for (int i = 0; i < MAX_VERTEX; i++)

			for (int j = 0; j < MAX_VERTEX; j++)

				adjMatrix[i][j] = INFINITY;

	}

	public void addVertex(char label) {

		vertexList[nVerts++] = new Vertex(label);

	}

	// ����ͼ

	public void addOneEdge(int start, int end, int weight) {

		adjMatrix[start][end] = weight;

	}

	public void dijkstra() {

		int startTree = 0;

		vertexList[startTree].isInTree = true;

		nTree = 1;

		for (int j = 0; j < nVerts; j++) {

			int tempDist = adjMatrix[startTree][j];

			sPath[j] = new DistPare(startTree, tempDist);

		}

		while (nTree < nVerts) {

			int indexMin = getMin();

			int minDist = sPath[indexMin].distance;

			if (minDist == INFINITY) {

				System.out.println("���޷�����Ķ���");

			}

			else {

				currentVertex = indexMin;

				startToCurrent = sPath[indexMin].distance;

			}

			vertexList[currentVertex].isInTree = true;

			nTree++;

			adjust_sPath();

		}

		displaypaths();

	}

	private void displaypaths() {

		for (int j = 0; j < nVerts; j++) {

			System.out.print(vertexList[j].label + "=");

			if (sPath[j].distance == INFINITY)

				System.out.print("inf");

			else

				System.out.print(sPath[j].distance);

			char parent = vertexList[sPath[j].parentVertex].label;

			System.out.print("(" + parent + ") ");

		}

		System.out.println(" ");

	}

	private void adjust_sPath() {

		int column = 1;

		while (column < nVerts) {

			if (vertexList[column].isInTree) {

				column++;

				continue;

			}

			int currentToFringe = adjMatrix[currentVertex][column];

			int startToFringe = startToCurrent + currentToFringe;

			int sPathDist = sPath[column].distance;

			if (startToFringe < sPathDist) {

				sPath[column].parentVertex = currentVertex;

				sPath[column].distance = startToFringe;

			}

			column++;

		}

	}

	private int getMin() {

		int minDist = INFINITY;

		int indexMin = 0;

		for (int j = 0; j < nVerts; j++) {

			if (!vertexList[j].isInTree && sPath[j].distance < minDist) {

				minDist = sPath[j].distance;

				indexMin = j;

			}

		}

		return indexMin;

	}

}