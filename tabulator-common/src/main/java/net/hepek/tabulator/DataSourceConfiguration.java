package net.hepek.tabulator;

public class DataSourceConfiguration {

	private String uri;
	private int refreshTimeSeconds;

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getRefreshTimeSeconds() {
		return refreshTimeSeconds;
	}

	public void setRefreshTimeSeconds(int refreshTimeSeconds) {
		if(refreshTimeSeconds < 0){
			throw new IllegalArgumentException("Refresh must be >= 0");
		}
		this.refreshTimeSeconds = refreshTimeSeconds;
	}
	
}
