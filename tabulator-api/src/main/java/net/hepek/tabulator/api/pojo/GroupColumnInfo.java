package net.hepek.tabulator.api.pojo;

import java.util.List;

public class GroupColumnInfo extends ColumnInfo {

	private List<ColumnInfo> children;

	public GroupColumnInfo() {
		super();
		this.setType(ColumnType.GROUP);
	}

	public List<ColumnInfo> getChildren() {
		return children;
	}

	public void setChildren(List<ColumnInfo> children) {
		this.children = children;
	}
	
}
