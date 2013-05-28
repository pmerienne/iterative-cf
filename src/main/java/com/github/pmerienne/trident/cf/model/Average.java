package com.github.pmerienne.trident.cf.model;

import java.io.Serializable;

public class Average implements Serializable {

	private static final long serialVersionUID = 8546845355765640295L;

	public final double sum;
	public final long count;

	public Average(double sum, long count) {
		this.sum = sum;
		this.count = count;
	}

	public Average add(double value) {
		return new Average(this.sum + value, this.count + 1);
	}

	public double getMean() {
		return this.sum / this.count;
	}

	public double getSum() {
		return sum;
	}

	public long getCount() {
		return count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		long temp;
		temp = Double.doubleToLongBits(sum);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Average other = (Average) obj;
		if (count != other.count)
			return false;
		if (Double.doubleToLongBits(sum) != Double.doubleToLongBits(other.sum))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Average [sum=" + sum + ", count=" + count + "]";
	}

}
