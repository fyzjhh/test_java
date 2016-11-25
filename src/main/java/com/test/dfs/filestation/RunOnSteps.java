package com.filestation.touchMFS;

/**
 * User: xzhou
 * Date: 11-12-2
 */
public class RunOnSteps {

	private int totalLength = -1;
	private int position = -1;
	private int stepLength = -1;

	private int current = -1;
	private int length = -1;

	public RunOnSteps(int totalLength, int startPosition, int stepLength) {

		assert (totalLength > 0);
		assert (stepLength > 0);
		assert (startPosition >= 0);

		assert (startPosition < totalLength);
		assert (stepLength > 0);

		this.totalLength = totalLength;
		this.position = startPosition;
		this.stepLength = stepLength;

	}

	public int getOffset() {
		assert (this.totalLength > 0);
		assert (this.stepLength > 0);
		assert (this.position >= 0);

		return this.current;
	}

	public int getLength() {
		assert (this.totalLength > 0);
		assert (this.stepLength > 0);
		assert (this.position >= 0);

		return this.length;
	}

	public boolean next() {

		assert (this.totalLength > 0);
		assert (this.stepLength > 0);
		assert (this.position >= 0);

		if (this.position >= this.totalLength) {

			totalLength = -1;
			position = -1;
			stepLength = -1;
			current = -1;
			length = -1;

			return false;
		}

		if (this.current == -1) {
			this.current = this.position;
		} else {
			this.current += this.length;
		}

		if (this.current + this.stepLength > this.totalLength) {
			this.length = this.totalLength - this.current;
		} else {
			this.length = this.stepLength;
		}

		this.position += this.length;

		return true;
	}
}
