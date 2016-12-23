package net.hepek.tabulator.api.pojo;

public class DirectoryInfo {

	private String absolutePath;
	private long sizeBytes;
	private long timeCreated;
	private int countProcessedFilesInsideDirectory;
	private long summedTotalSizeOfFilesBytesUnderDirectory;
	private int countProcessedFilesUnderDirectory;

	public String getAbsolutePath() {
		return absolutePath;
	}

	public void setAbsolutePath(String absolutePath) {
		this.absolutePath = absolutePath;
	}

	public long getSizeBytes() {
		return sizeBytes;
	}

	public void setSizeBytes(long sizeBytes) {
		this.sizeBytes = sizeBytes;
	}

	public long getTimeCreated() {
		return timeCreated;
	}

	public void setTimeCreated(long timeCreated) {
		this.timeCreated = timeCreated;
	}

	public int getCountProcessedFilesInsideDirectory() {
		return countProcessedFilesInsideDirectory;
	}

	public void setCountProcessedFilesInsideDirectory(int countProcessedFilesInsideDirectory) {
		this.countProcessedFilesInsideDirectory = countProcessedFilesInsideDirectory;
	}

	public long getSummedTotalSizeOfFilesBytesUnderDirectory() {
		return summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public void setSummedTotalSizeOfFilesBytesUnderDirectory(long summedTotalSizeOfFilesBytesUnderDirectory) {
		this.summedTotalSizeOfFilesBytesUnderDirectory = summedTotalSizeOfFilesBytesUnderDirectory;
	}

	public int getCountProcessedFilesUnderDirectory() {
		return countProcessedFilesUnderDirectory;
	}

	public void setCountProcessedFilesUnderDirectory(int countProcessedFilesUnderDirectory) {
		this.countProcessedFilesUnderDirectory = countProcessedFilesUnderDirectory;
	}

}
