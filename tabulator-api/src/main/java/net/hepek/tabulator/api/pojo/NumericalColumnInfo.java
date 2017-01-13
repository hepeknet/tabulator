package net.hepek.tabulator.api.pojo;

public class NumericalColumnInfo extends ColumnInfo {

	private long minValue;
	private long maxValue;

	public NumericalColumnInfo() {
		super();
		this.setType(ColumnType.INT);
	}

	public long getMinValue() {
		return minValue;
	}

	public void setMinValue(long minValue) {
		this.minValue = minValue;
	}

	public long getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(long maxValue) {
		this.maxValue = maxValue;
	}

}
