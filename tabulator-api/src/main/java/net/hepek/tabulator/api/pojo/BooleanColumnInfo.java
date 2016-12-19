package net.hepek.tabulator.api.pojo;

public class BooleanColumnInfo extends ColumnInfo {

	private long numberOfTrue;
	private long numberOfFalse;

	public BooleanColumnInfo() {
		super();
		this.setType(ColumnType.BOOLEAN);
	}

	public long getNumberOfTrue() {
		return numberOfTrue;
	}

	public void setNumberOfTrue(long numberOfTrue) {
		this.numberOfTrue = numberOfTrue;
	}

	public long getNumberOfFalse() {
		return numberOfFalse;
	}

	public void setNumberOfFalse(long numberOfFalse) {
		this.numberOfFalse = numberOfFalse;
	}

}
