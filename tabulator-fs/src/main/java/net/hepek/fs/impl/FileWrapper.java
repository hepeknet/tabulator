package net.hepek.fs.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;

public class FileWrapper {
	
	private Path localFile;
	private FileStatus hdfsFile;
	private FileSystem hdfs;
	
	public FileWrapper(Path lf){
		if(lf == null){
			throw new IllegalArgumentException("Local file must not be null");
		}
		this.localFile = lf;
	}

	public FileWrapper(FileStatus hf, FileSystem hdfs){
		if(hf == null){
			throw new IllegalArgumentException("HDFS file must not be null");
		}
		if(hdfs == null){
			throw new IllegalArgumentException("HDFS FS must not be null");
		}
		this.hdfsFile = hf;
		this.hdfs = hdfs;
	}
	
	public long getLastModificationTime()throws IOException{
		if(localFile != null){
			final BasicFileAttributes dirAttrs = Files.readAttributes(localFile, BasicFileAttributes.class);
			final long dirModificationTime = dirAttrs.lastModifiedTime().toMillis();
			return dirModificationTime;
		} else if(hdfsFile != null){
			final long dirModificationTime = hdfsFile.getModificationTime();
			return dirModificationTime;
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public long getCreationTime() throws IOException {
		if(localFile != null){
			final BasicFileAttributes dirAttrs = Files.readAttributes(localFile, BasicFileAttributes.class);
			return dirAttrs.creationTime().toMillis();
		} else if(hdfsFile != null){
			final long dirModificationTime = hdfsFile.getModificationTime();
			return dirModificationTime;
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public String getFullPath() throws IOException {
		if(localFile != null){
			final String fullDirPath = localFile.toFile().getAbsolutePath();
			return fullDirPath;
		} else if(hdfsFile != null){
			final String fullDirPath = getFullFileName(hdfsFile);
			return fullDirPath;
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public long getFileSize() {
		if(localFile != null){
			return localFile.toFile().length();
		} else if(hdfsFile != null){
			return hdfsFile.getLen();
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	private String getFullFileName(FileStatus entry) {
		return entry.getPath().toString();
	}
	
	public String getNameOnly(){
		if(localFile != null){
			return localFile.toFile().getName();
		} else if(hdfsFile != null){
			return hdfsFile.getPath().getName();
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public boolean isHidden(){
		final String name = getNameOnly();
		return name.startsWith(".") || name.startsWith("_temp");
	}
	
	public FileWrapper[] listChildren() throws IOException{
		if(localFile != null){
			final DirectoryStream<Path> stream = Files.newDirectoryStream(localFile);
			final List<FileWrapper> fwList = new LinkedList<>();
			for(final Path p : stream){
				final FileWrapper fw = new FileWrapper(p);
				fwList.add(fw);
			}
			stream.close();
			return (FileWrapper[]) fwList.toArray();
		} else if(hdfsFile != null){
			final FileStatus[] listStatus = hdfs.listStatus(hdfsFile.getPath(), new PathFilter() {
				
				@Override
				public boolean accept(org.apache.hadoop.fs.Path path) {
					final String name = path.getName();
					return !ParquetUtil.isHiddenFile(name);
				}
			});
			final FileWrapper[] fwList = new FileWrapper[listStatus.length];
			for(int i=0;i<listStatus.length;i++){
				final FileStatus fs = listStatus[i];
				final FileWrapper fw = new FileWrapper(fs, hdfs);
				fwList[i] = fw;
			}
			return fwList;
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public boolean isDirectory(){
		if(localFile != null){
			return localFile.toFile().isDirectory();
		} else if(hdfsFile != null){
			return hdfsFile.isDirectory();
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
	public URI toURI(){
		if(localFile != null){
			return localFile.toUri();
		} else if(hdfsFile != null){
			return hdfsFile.getPath().toUri();
		} else {
			throw new IllegalStateException("No file set...");
		}
	}
	
}
