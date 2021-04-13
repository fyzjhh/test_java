package com.test.algorithm.other;

import java.util.ArrayList;
import java.util.Random;

public class RedBlackTree {

	private final Node NIL = new Node(null, null, null, Color.BLACK, -1);
	private Node root;

	public RedBlackTree() {
		root = NIL;
	}

	public RedBlackTree(Node root) {
		this.root = root;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		RedBlackTree tree = new RedBlackTree();
		Random random = new Random();
		Node tmpNode = null;

		ArrayList ar = new ArrayList();
		for (int i = 0; i < 10; i++) {

			tmpNode = new Node();
			int nodeVal = random.nextInt(1000);
			int nodeColInt = random.nextInt(10);
			Color nodeCol = (nodeColInt % 2 == 0) ? Color.RED : Color.BLACK;
			tmpNode.setValue(nodeVal);
			tmpNode.setColor(nodeCol);

			if (ar.contains(nodeVal)) {
				System.out.println("node already exist:" + tmpNode.toString());
			} else {
				System.out.println("add node :" + tmpNode.toString());
				tree.rbInsert(tmpNode);
				ar.add(nodeVal);
			}

		}

		tree.display(tree.root);
	}

	public void rbInsert(Node node) {

		Node previous = NIL;
		Node temp = root;

		while (temp != NIL) {
			previous = temp;
			if (temp.getValue() < node.getValue()) {
				temp = temp.getRight();
			} else {
				temp = temp.getLeft();
			}
		}
		node.setParent(previous);

		if (previous == NIL) {
			root = node;
			root.setParent(NIL);
		} else if (previous.getValue() > node.getValue()) {
			previous.setLeft(node);
		} else {
			previous.setRight(node);
		}

		node.setLeft(NIL);
		node.setRight(NIL);
		node.setColor(Color.RED);
		rb_Insert_Fixup(node);

	}

	private void rb_Insert_Fixup(Node node) {

		while (node.getParent().getColor() == Color.RED) {

			if (node.getParent() == node.getParent().getParent().getLeft()) {

				Node rightNuncle = node.getParent().getParent().getRight();

				if (rightNuncle.getColor() == Color.RED) { // Case 1

					rightNuncle.setColor(Color.BLACK);
					node.getParent().setColor(Color.BLACK);
					node.getParent().getParent().setColor(Color.RED);
					node = node.getParent().getParent();

				} else if (node == node.getParent().getRight()) { // case 2

					node = node.getParent();
					leftRotate(node);

				} else { // case 3

					node.getParent().setColor(Color.BLACK);
					node.getParent().getParent().setColor(Color.RED);

					rightRotate(node.getParent().getParent());

				}

			} else {

				Node leftNuncle = node.getParent().getParent().getLeft();

				if (leftNuncle.getColor() == Color.RED) { // case 4

					leftNuncle.setColor(Color.BLACK);
					node.getParent().setColor(Color.BLACK);
					node.getParent().getParent().setColor(Color.RED);
					node = node.getParent().getParent();

				} else if (node == node.getParent().getLeft()) { // case 5

					node = node.getParent();
					rightRotate(node);

				} else { // case 6

					node.getParent().setColor(Color.BLACK);
					node.getParent().getParent().setColor(Color.RED);
					leftRotate(node.getParent().getParent());

				}

			}

		}

		root.setColor(Color.BLACK);

	}

	public Node rbDelete(int data) {

		Node node = search(data);
		Node temp = NIL;
		Node child = NIL;
		if (node == null) {
			return null;
		} else {
			if (node.getLeft() == NIL || node.getRight() == NIL) {
				temp = node;
			} else {
				temp = successor(node);
			}

			if (temp.getLeft() != NIL) {
				child = temp.getLeft();
			} else {
				child = temp.getRight();
			}

			child.setParent(temp.getParent());

			if (temp.getParent() == NIL) {
				root = child;
			} else if (temp == temp.getParent().getLeft()) {
				temp.getParent().setLeft(child);
			} else {
				temp.getParent().setRight(child);
			}

			if (temp != node) {
				node.setValue(temp.getValue());
			}

			if (temp.getColor() == Color.BLACK) {
				rb_Delete_Fixup(child);
			}
			return temp;
		}

	}

	private void rb_Delete_Fixup(Node node) {

		while (node != root && node.getColor() == Color.BLACK) {

			if (node == node.getParent().getLeft()) {

				Node rightBrother = node.getParent().getRight();
				if (rightBrother.getColor() == Color.RED) { // case 1
															// node�ڵ�Ϊ���ӣ�node�ڵ���ֵ�ΪRED
					rightBrother.setColor(Color.BLACK);
					node.getParent().setColor(Color.RED);
					leftRotate(node.getParent());
					rightBrother = node.getParent().getRight();
				}

				if (rightBrother.getLeft().getColor() == Color.BLACK
						&& rightBrother.getRight().getColor() == Color.BLACK) {
					rightBrother.setColor(Color.RED);
					node = node.getParent();
				} else if (rightBrother.getRight().getColor() == Color.BLACK) {
					rightBrother.getLeft().setColor(Color.BLACK);
					rightBrother.setColor(Color.RED);
					rightRotate(rightBrother);
					rightBrother = node.getParent().getRight();
				} else {
					rightBrother.setColor(node.getParent().getColor());
					node.getParent().setColor(Color.BLACK);
					rightBrother.getRight().setColor(Color.BLACK);
					leftRotate(node.getParent());
					node = root;
				}

			} else {

				Node leftBrother = node.getParent().getLeft();
				if (leftBrother.getColor() == Color.RED) {
					leftBrother.setColor(Color.BLACK);
					node.getParent().setColor(Color.RED);
					rightRotate(node.getParent());
					leftBrother = node.getParent().getLeft();
				}

				if (leftBrother.getLeft().getColor() == Color.BLACK
						&& leftBrother.getRight().getColor() == Color.BLACK) {
					leftBrother.setColor(Color.RED);
					node = node.getParent();

				} else if (leftBrother.getLeft().getColor() == Color.BLACK) {

					leftBrother.setColor(Color.RED);
					leftBrother.getRight().setColor(Color.BLACK);
					leftRotate(leftBrother);
					leftBrother = node.getParent().getLeft();

				} else {

					leftBrother.setColor(node.getParent().getColor());
					node.getParent().setColor(Color.BLACK);
					leftBrother.getLeft().setColor(Color.BLACK);
					rightRotate(node.getParent());
					node = root;

				}

			}

		}

		node.setColor(Color.BLACK);
	}


	public Node successor(Node node) {

		Node rightChild = node.getRight();
		if (rightChild != NIL) {
			Node previous = null;
			while (rightChild != NIL) {
				previous = rightChild;
				rightChild = rightChild.getLeft();
			}
			return previous;
		} else {

			Node parent = node.getParent();
			while (parent != NIL && node != parent.getLeft()) {
				node = parent;
				parent = parent.getParent();
			}

			return parent;

		}

	}

	public Node search(int data) {
		Node temp = root;

		while (temp != NIL) {
			if (temp.getValue() == data) {
				return temp;
			} else if (data < temp.getValue()) {
				temp = temp.getLeft();
			} else {
				temp = temp.getRight();
			}
		}
		return null;
	}

	private void leftRotate(Node node) {

		Node rightNode = node.getRight();

		node.setRight(rightNode.getLeft());
		if (rightNode.getLeft() != NIL) {
			rightNode.getLeft().setParent(node);
		}
		rightNode.setParent(node.getParent());

		if (node.getParent() == NIL) {
			rightNode = root;
		} else if (node == node.getParent().getLeft()) {
			node.getParent().setLeft(rightNode);
		} else {
			node.getParent().setRight(rightNode);
		}

		rightNode.setLeft(node);
		node.setParent(rightNode);

	}

	private void rightRotate(Node node) {

		Node leftNode = node.getLeft();
		node.setLeft(leftNode.getRight());

		if (leftNode.getRight() != null) {
			leftNode.getRight().setParent(node);
		}

		leftNode.setParent(node.getParent());

		if (node.getParent() == NIL) {
			root = leftNode;
		} else if (node == node.getParent().getLeft()) {
			node.getParent().setLeft(leftNode);
		} else {
			node.getParent().setRight(leftNode);
		}

		leftNode.setRight(node);
		node.setParent(leftNode);

	}

	StringBuilder sb = new StringBuilder();

	void display(Node node) {
		if (node != null) {
			System.out.printf("%s", node.toString());
			if (node.getLeft() != null || node.getRight() != null) {
				System.out.printf("(");
				display(node.getLeft());
				if (node.getRight() != null)
					System.out.printf(",");
				display(node.getRight());
				System.out.printf(")");
			}
		}
	}

	public void preOrder() {
		System.out.println("����������£�");
		preOrder(root);
	}

	private void preOrder(Node node) {
		if (node != NIL) {
			System.out.print(node.toString() + "     ");
			preOrder(node.getLeft());
			preOrder(node.getRight());
		}

	}

	public void printTree() {
		System.out.println("����������£�");
		inOrder(root);
	}

	private void inOrder(Node node) {

		if (node != NIL) {
			inOrder(node.getLeft());
			System.out.println(node.toString() + "     ");
			inOrder(node.getRight());
		}

	}

	public Node getNIL() {
		return NIL;
	}

}

class Node {
	private Node left;
	private Node right;
	private Node parent;
	private Color color;
	private int value;

	public Node(Node left, Node right, Node parent, Color color, int value) {
		super();
		this.left = left;
		this.right = right;
		this.parent = parent;
		this.color = color;
		this.value = value;
	}

	public Node() {
	}

	public Node(int value) {
		this(null, null, null, null, value);
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

	public Node getParent() {
		return parent;
	}

	public void setParent(Node parent) {
		this.parent = parent;
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public String toString() {
		return "[" + value + "," + color + "]";

	}

}

enum Color {
	RED, BLACK
}
