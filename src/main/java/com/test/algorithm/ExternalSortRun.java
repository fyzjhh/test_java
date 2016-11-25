package com.test.algorithm;

import java.util.Scanner;

public class ExternalSortRun {
	private int count;
	private String next;
	private Scanner scanner;

	public ExternalSortRun(Scanner scanner, int maxLength) {
		count = maxLength;
		this.scanner = scanner;
		if (scanner.hasNext()) {
			next = scanner.nextLine();
		} else {
			count = 0;
		}
	}

	public boolean hasNext() {
		return count > 0;
	}

	public String next() {
		String result = next;
		count--;
		if (count > 0) {
			if (scanner.hasNext()) {
				next = scanner.nextLine();
			} else {
				count = 0;
			}
		}
		return result;
	}

	public String peek() {
		return next;
	}
}