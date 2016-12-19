package net.hepek.tabulator.api.pojo;

public class StringColumnInfo extends ColumnInfo {

	private int maxLength;

	public StringColumnInfo() {
		super();
		this.setType(ColumnType.STRING);
	}

	public int getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}

}
