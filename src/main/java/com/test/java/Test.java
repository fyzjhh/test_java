package com.test.java;
class Node {

	Node left;
	Node right;
	String name;

	public String toString() {
		return "name=[" + name + "]";
	}

	public Node(String name) {
		this.name = name;
	}

	public Node getLeft() {
		return left;
	}

	public void setLeft(Node left) {
		this.left = left;
	}

	public Node getRight() {
		return right;
	}

	public void setRight(Node right) {
		this.right = right;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}

public class Test {

	public static void preTra(Node node) {

		if (node != null) {

			preTra(node.getLeft());
			System.out.println(node.toString());
			preTra(node.getRight());

		}

	}

	public static void preTraNoCur(Node node) {

		java.util.Stack<Node> s = new java.util.Stack<Node>();
		Node root = node;
		while (node != null || false==s.isEmpty()) {
			while (node != null) {

				System.out.println(node.toString());
				s.push(node);
				node = node.getLeft();
			}
			node = s.pop();
			node = node.getRight();
		}
	}

	public static void main(String[] args) {

		Node node_2_0 = new Node("node_2_0");
		Node node_2_1 = new Node("node_2_1");
		Node node_2_2 = new Node("node_2_2");
		Node node_2_3 = new Node("node_2_3");

		Node node_1_0 = new Node("node_1_0");
		Node node_1_1 = new Node("node_1_1");
		Node node_0_0 = new Node("node_0_0");

		node_1_0.setLeft(node_2_0);
		node_1_0.setRight(node_2_1);

		node_1_1.setLeft(node_2_2);
		node_1_1.setRight(node_2_3);

		node_0_0.setLeft(node_1_0);
		node_0_0.setRight(node_1_1);

		preTraNoCur(node_0_0);
	}
}
