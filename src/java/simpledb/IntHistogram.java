package simpledb;

/**
 * A class to represent a fixed-width
 * histogram over a single integer-based field.
 */
public class IntHistogram {
	private final int[] buckets;
	private final int max;
	private final int min;
	private final int length;
	private int count;

	/**
	 * Create a new IntHistogram.
	 * <p>
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * <p>
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * <p>
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed.  For example, you shouldn't simply store every value that you see in a sorted list.
	 *
	 * @param buckets The number of buckets to split the input value into.
	 * @param min     The minimum integer value that will ever be passed to this class for histogramming
	 * @param max     The maximum integer value that will ever be passed to this class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		this.buckets = new int[buckets];
		this.min = min;
		this.max = max;
		this.length = buckets;
		this.count = 0;
	}

	private int indexOfValue(int value) {
		if (value < min) {
			return 0;
		} else if (value > max) {
			return length - 1;
		} else {
			return (value - min) * (length - 1) / (max - min);
		}
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 *
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		buckets[indexOfValue(v)] += 1;
		count += 1;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * <p>
	 * For example, if "op" is "GREATER_THAN" and "v" is 5,
	 * return your estimate of the fraction of elements that are greater than 5.
	 *
	 * @param op Operator
	 * @param v  Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		double result = 0;
		double fraction[] = new double[length];
		final int index = indexOfValue(v);
		switch (op) {
			case EQUALS:
			case NOT_EQUALS:
				fraction[index] =1.0 * (length - 1) / (max - min);
				break;
			case LESS_THAN:
			case LESS_THAN_OR_EQ:
			case GREATER_THAN:
			case GREATER_THAN_OR_EQ:
				for (int i = 0; i < index; i++) {
					fraction[i] = 1;
				}
				fraction[index] = (v - (min + 1.0 * (max - min) / (length - 1) * index)) * (length - 1) / (max - min);
				break;

		}
		switch (op) {
			case NOT_EQUALS:
			case GREATER_THAN:
				for (int i = 0; i < length; i++) {
					fraction[i] = 1 - fraction[i];
				}
				break;
			case LESS_THAN_OR_EQ:
				fraction[index] += 1.0 * (length - 1) / (max - min);
				break;
			case GREATER_THAN_OR_EQ:
				for (int i = 0; i < length; i++) {
					fraction[i] = 1 - fraction[i];
				}
				fraction[index] += 1.0 * (length - 1) / (max - min);
				break;
			default:
				break;

		}
		double sum = 0;
		for (int i = 0; i < length; i++) {
			sum += fraction[i] * buckets[i];
		}
		return sum / count;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * <p>
	 * This is not an indispensable method to implement the basic
	 * join optimization. It may be needed if you want to
	 * implement a more efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here
		return 1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		// some code goes here
		return null;
	}
}
