package net.hepek.fs.impl;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hepek.tabulator.api.ds.DataSourceProcessor;
import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.DataSourceType;
import net.hepek.tabulator.api.pojo.FileType;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;
import net.hepek.tabulator.api.storage.Storage;

public class FSDataSourceProcessor implements DataSourceProcessor {

	private static final String HDFS_PREFIX = "hdfs:";
	private static final String FILE_PREFIX = "file:";
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public boolean understandsDataSourceUri(String uri) {
		if (uri != null) {
			return uri.startsWith(FILE_PREFIX) || uri.startsWith(HDFS_PREFIX) || uri.startsWith("/");
		}
		return false;
	}

	@Override
	public void processDataSource(String uri, Storage saver) {
		if (saver == null) {
			throw new IllegalArgumentException("Saver must not be null");
		}
		log.debug("Processing data source {}", uri);
		try {
			if (isLocalFs(uri)) {
				processLocalDS(uri, saver);
			} else {
				processHDFS(uri, saver);
			}
		} catch (final Exception ie) {
			log.error("Exception while processing {}", uri, ie);
		}
	}

	private void processLocalDS(String uri, Storage storage) throws IOException {
		log.debug("Processing local fs {}", uri);
		final DataSourceInfo dsi = new DataSourceInfo();
		dsi.setAccessURI(uri);
		dsi.setType(DataSourceType.FS);
		final Path dir = Paths.get(uri);
		if (!dir.toFile().isDirectory()) {
			log.warn("{} is not directory. Will not process it.", uri);
			return;
		}
		final ParquetConverter converter = new ParquetConverter();
		final FileProcessingOutput res = processLocalDirectory(dir, storage, converter, dsi);
		final long totalSizeInBytes = res.sizeBytes;
		final long oldestCreationTime = res.timeCreated;
		final long lastUpdateTime = res.timeUpdated;
		dsi.setSizeInBytes(totalSizeInBytes);
		dsi.setOldestItemCreationTime(oldestCreationTime);
		dsi.setLastUpdateTime(lastUpdateTime);
		storage.save(dsi);
		log.debug("Successfully processed {}", uri);
	}

	private FileProcessingOutput processLocalDirectory(Path dir, Storage storage, ParquetConverter converter,
			DataSourceInfo dsi) throws IOException {
		long totalSizeInBytes = 0;
		long oldestCreationTime = 0;
		long lastUpdateTime = 0;
		final FileProcessingOutput res = new FileProcessingOutput();
		final boolean shouldProcessDir = shouldProcessLocalDirectory(dir, storage);
		if (!shouldProcessDir) {
			log.debug("Directory {} was not modified since last check. Will not process it",
					dir.toFile().getAbsolutePath());
			return res;
		}
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.{parquet, pq}")) {
			for (final Path entry : stream) {
				final File f = entry.toFile();
				if (f.isDirectory()) {
					final FileProcessingOutput dirOut = processLocalDirectory(entry, storage, converter, dsi);
					if (oldestCreationTime < dirOut.timeCreated) {
						oldestCreationTime = dirOut.timeCreated;
					}
					if (dirOut.timeUpdated > lastUpdateTime) {
						lastUpdateTime = dirOut.timeUpdated;
					}
					totalSizeInBytes += dirOut.sizeBytes;
				} else {
					try {
						final FileProcessingOutput out = processLocalFile(converter, entry, storage, dsi);
						if (oldestCreationTime < out.timeCreated) {
							oldestCreationTime = out.timeCreated;
						}
						if (out.timeUpdated > lastUpdateTime) {
							lastUpdateTime = out.timeUpdated;
						}
						totalSizeInBytes += out.sizeBytes;
					} catch (final Exception exc) {
						log.warn("Exception while parsing file {}", entry, exc);
					}
				}
			}
		} catch (final DirectoryIteratorException ex) {
			throw ex.getCause();
		}
		res.sizeBytes = totalSizeInBytes;
		res.timeCreated = oldestCreationTime;
		res.timeUpdated = lastUpdateTime;
		saveLastModificationTimeLocal(dir, storage);
		return res;
	}

	private FileProcessingOutput processHDFSDirectory(FileStatus dir, Storage storage, ParquetConverter converter,
			DataSourceInfo dsi, FileSystem hdfs) throws IOException {
		long totalSizeInBytes = 0;
		long oldestCreationTime = 0;
		long lastUpdateTime = 0;
		final FileProcessingOutput res = new FileProcessingOutput();
		final boolean shouldProcessDir = shouldProcessHDFSDirectory(dir, storage);
		if (!shouldProcessDir) {
			final String dirName = getFullFileName(dir);
			log.debug("Directory {} was not modified since last check. Will not process it", dirName);
			return res;
		}
		final FileStatus[] children = hdfs.listStatus(dir.getPath());
		for (final FileStatus entry : children) {
			if (entry.isDirectory()) {
				final FileProcessingOutput dirOut = processHDFSDirectory(entry, storage, converter, dsi, hdfs);
				if (oldestCreationTime < dirOut.timeCreated) {
					oldestCreationTime = dirOut.timeCreated;
				}
				if (dirOut.timeUpdated > lastUpdateTime) {
					lastUpdateTime = dirOut.timeUpdated;
				}
				totalSizeInBytes += dirOut.sizeBytes;
			} else {
				try {
					final FileProcessingOutput out = processHDFSFile(converter, entry, storage, dsi);
					if (oldestCreationTime < out.timeCreated) {
						oldestCreationTime = out.timeCreated;
					}
					if (out.timeUpdated > lastUpdateTime) {
						lastUpdateTime = out.timeUpdated;
					}
					totalSizeInBytes += out.sizeBytes;
				} catch (final Exception exc) {
					log.warn("Exception while parsing file {}", entry, exc);
				}
			}
		}
		res.sizeBytes = totalSizeInBytes;
		res.timeCreated = oldestCreationTime;
		res.timeUpdated = lastUpdateTime;
		saveLastModificationTimeHDFS(dir, storage);
		return res;
	}

	private void saveLastModificationTimeLocal(Path dir, Storage storage) throws IOException {
		final BasicFileAttributes dirAttrs = Files.readAttributes(dir, BasicFileAttributes.class);
		final long dirModificationTime = dirAttrs.lastModifiedTime().toMillis();
		final String dirPath = dir.toFile().getAbsolutePath();
		storage.saveLastModified(dirPath, dirModificationTime);
		log.debug("Saved last modification time {}={}", dirPath, dirModificationTime);
	}

	private void saveLastModificationTimeHDFS(FileStatus dir, Storage storage) throws IOException {
		final long dirModificationTime = dir.getModificationTime();
		final String dirPath = getFullFileName(dir);
		storage.saveLastModified(dirPath, dirModificationTime);
		log.debug("Saved last modification time {}={}", dirPath, dirModificationTime);
	}

	private boolean shouldProcessLocalDirectory(Path dir, Storage storage) throws IOException {
		final String dirPath = dir.toFile().getAbsolutePath();
		log.debug("Checking if {} should be processed", dirPath);
		if (isHidden(dirPath)) {
			log.debug("{} is hidden - will not process it", dirPath);
			return false;
		}
		final BasicFileAttributes dirAttrs = Files.readAttributes(dir, BasicFileAttributes.class);
		final long dirModificationTime = dirAttrs.lastModifiedTime().toMillis();
		final long lastRememberedModificationTime = storage.getLastModified(dirPath);
		final boolean shouldProcess = lastRememberedModificationTime < dirModificationTime;
		log.debug("Should process {} = {}", dirPath, shouldProcess);
		return shouldProcess;
	}

	private boolean isHidden(String name) {
		return name.startsWith(".");
	}

	private boolean shouldProcessHDFSDirectory(FileStatus dir, Storage storage) throws IOException {
		final String dirPath = getFullFileName(dir);
		log.debug("Checking if {} should be processed", dirPath);
		if (isHidden(dirPath)) {
			log.debug("{} is hidden - will not process it", dirPath);
			return false;
		}
		final long dirModificationTime = dir.getModificationTime();
		final long lastRememberedModificationTime = storage.getLastModified(dirPath);
		final boolean shouldProcess = lastRememberedModificationTime < dirModificationTime;
		log.debug("Should process {} = {}", dirPath, shouldProcess);
		return shouldProcess;
	}

	private FileProcessingOutput processLocalFile(ParquetConverter converter, Path entry, Storage storage,
			DataSourceInfo dsi) throws Exception {
		final File f = entry.toFile();
		final FileProcessingOutput out = new FileProcessingOutput();
		SchemaInfo si = null;
		try {
			final URI uri = entry.toUri();
			final boolean isParquetFile = isParquetFile(uri.toString());
			if (isParquetFile) {
				si = converter.parseParquetFile(uri);
			}
		} catch (final Exception exc) {
			log.warn("Exception while parsing {} - details {}", entry, exc.getMessage());
		}
		if (si != null) {
			si.addDatasource(dsi);
			storage.save(si);
			final FileWithSchema fws = new FileWithSchema();
			final BasicFileAttributes attrs = Files.readAttributes(entry, BasicFileAttributes.class);
			final long timeCreated = attrs.creationTime().toMillis();
			final long timeUpdated = attrs.lastModifiedTime().toMillis();
			fws.setSizeBytes(f.length());
			fws.setTimeCreated(timeCreated);
			out.timeCreated = timeCreated;
			out.timeUpdated = timeUpdated;
			out.sizeBytes = f.length();
			fws.setType(FileType.PARQUET);
			fws.setAbsolutePath(f.getAbsolutePath());
			fws.setSchemaId(si.getId());
			storage.save(fws);
			log.debug("Successfully parsed {}", entry);
		}
		return out;
	}

	private FileProcessingOutput processHDFSFile(ParquetConverter converter, FileStatus entry, Storage storage,
			DataSourceInfo dsi) throws Exception {
		final FileProcessingOutput out = new FileProcessingOutput();
		SchemaInfo si = null;
		try {
			final URI uri = entry.getPath().toUri();
			final boolean isParquetFile = isParquetFile(uri.toString());
			if (isParquetFile) {
				si = converter.parseParquetFile(uri);
			}
		} catch (final Exception exc) {
			log.warn("Exception while parsing {} - details {}", entry, exc.getMessage());
		}
		if (si != null) {
			si.addDatasource(dsi);
			storage.save(si);
			final FileWithSchema fws = new FileWithSchema();
			fws.setSizeBytes(entry.getLen());
			fws.setTimeCreated(entry.getModificationTime());
			out.timeCreated = entry.getModificationTime();
			out.timeUpdated = entry.getModificationTime();
			out.sizeBytes = entry.getLen();
			fws.setType(FileType.PARQUET);
			fws.setAbsolutePath(getFullFileName(entry));
			fws.setSchemaId(si.getId());
			storage.save(fws);
			log.debug("Successfully parsed {}", entry);
		}
		return out;
	}

	private String getFullFileName(FileStatus entry) {
		return entry.getPath().toString();
	}

	class FileProcessingOutput {
		long timeCreated;
		long timeUpdated;
		long sizeBytes;
	}

	private void processHDFS(String uri, Storage storage) throws Exception {
		log.debug("Processing HDFS {}", uri);
		final DataSourceInfo dsi = new DataSourceInfo();
		dsi.setAccessURI(uri);
		dsi.setType(DataSourceType.HDFS);
		final FileSystem hdfs = new DistributedFileSystem();
		hdfs.initialize(new URI(uri), new Configuration());
		final org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(uri);
		final FileStatus dirFS = hdfs.getFileStatus(root);
		if (!dirFS.isDirectory()) {
			hdfs.close();
			log.warn("{} is not directory. Will not process it.", uri);
			return;
		}
		final ParquetConverter converter = new ParquetConverter();
		final FileProcessingOutput res = processHDFSDirectory(dirFS, storage, converter, dsi, hdfs);
		hdfs.close();
		final long totalSizeInBytes = res.sizeBytes;
		final long oldestCreationTime = res.timeCreated;
		final long lastUpdateTime = res.timeUpdated;
		dsi.setSizeInBytes(totalSizeInBytes);
		dsi.setOldestItemCreationTime(oldestCreationTime);
		dsi.setLastUpdateTime(lastUpdateTime);
		storage.save(dsi);
		log.debug("Successfully processed HDFS {}", uri);
	}

	private static boolean isLocalFs(String uri) {
		return uri.startsWith(FILE_PREFIX) || uri.startsWith("/");
	}

	private static boolean isParquetFile(String uri) {
		if (uri.startsWith(".")) {
			return false;
		}
		return uri.endsWith(".parquet") || uri.endsWith(".pqt") || uri.endsWith(".pq");
	}

}
