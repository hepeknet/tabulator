package net.hepek.tabulator.storage.es;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;

import net.hepek.tabulator.api.pojo.DataSourceInfo;
import net.hepek.tabulator.api.pojo.FileWithSchema;
import net.hepek.tabulator.api.pojo.SchemaInfo;

public class BulkElasticSearchStorage extends ElasticSearchStorage {

	private static final int DEFAULT_DELAY_MILLIS = 5 * 1000;

	private static final int INITIAL_DELAY_SECONDS = 10;
	private static final int PROCESS_EXPIRED_DELAY_SECONDS = 5;
	private static final Set<String> ALREADY_SAVED_SCHEMAS = new HashSet<>();
	private static final int MAX_ALREADY_SAVED_SCHEMAS = 5000;

	private final DelayQueue<PostponedWorkItem> queue = new DelayQueue<>();

	public BulkElasticSearchStorage(List<String> clusterNodes) {
		super(clusterNodes);
		saveExpiredItems();
	}

	private void saveExpiredItems() {
		final ScheduledExecutorService exec = Executors.newScheduledThreadPool(2);
		exec.scheduleWithFixedDelay(() -> {
			try {
				logger.debug("Checking if there are any expired items that can be saved...");
				final Collection<PostponedWorkItem> expired = new LinkedList<>();
				final int expiredCount = queue.drainTo(expired);
				logger.debug("Expired count = {}", expiredCount);
				if (expiredCount > 0) {
					final TransportClient bulkClient = createClient(this.clusterNodes);
					final BulkRequestBuilder bulkRequest = bulkClient.prepareBulk();
					int totalSchemas = 0;
					int totalFiles = 0;
					int totalDataSources = 0;
					int totalIMT = 0;
					int totalSchemasSkipped = 0;
					final Set<String> savedSchemaIdentifiers = new HashSet<>();
					for (final PostponedWorkItem pwi : expired) {
						final Object item = pwi.workItem;
						final byte[] json = mapper.writeValueAsBytes(item);
						if (item instanceof SchemaInfo) {
							final SchemaInfo schemaInfo = (SchemaInfo) item;
							final boolean schemaAlreadySaved = schemaAlreadySaved(schemaInfo);
							if (!schemaAlreadySaved) {
								bulkRequest.add(bulkClient.prepareIndex().setSource(json).setIndex(SCHEMA_INDEX)
										.setType(schemaInfo.getType()).setId(schemaInfo.getId()));
								totalSchemas += 1;
								savedSchemaIdentifiers.add(schemaInfo.getId());
							} else {
								totalSchemasSkipped += 1;
							}
						} else if (item instanceof FileWithSchema) {
							final FileWithSchema file = (FileWithSchema) item;
							bulkRequest.add(bulkClient.prepareIndex().setSource(json).setIndex(FILES_INDEX)
									.setType(file.getType().name()).setId(file.getAbsolutePath()));
							totalFiles += 1;
						} else if (item instanceof DataSourceInfo) {
							final DataSourceInfo ds = (DataSourceInfo) item;
							bulkRequest.add(bulkClient.prepareIndex().setSource(json).setIndex(DS_INDEX)
									.setType(ds.getType().name()).setId(ds.getAccessURI()));
							totalDataSources += 1;
						} else if (item instanceof InternalModificationTime) {
							final InternalModificationTime imt = (InternalModificationTime) item;
							bulkRequest
									.add(bulkClient.prepareIndex().setSource(json).setIndex(INTERNAL_MODIFICATION_INDEX)
											.setType(INTERNAL_MODIFICATION_TYPE).setId(imt.getUri()));
							totalIMT += 1;
						} else {
							logger.warn("Unknown instance type {}", item);
						}
					}
					final BulkResponse bulkResponse = bulkRequest.get();
					logger.debug(
							"Excuted bulk save with {} schemas, {} files, {} datasources and {} IMT. Skipped saving of {} schemas",
							totalSchemas, totalFiles, totalDataSources, totalIMT, totalSchemasSkipped);
					if (bulkResponse.hasFailures()) {
						logger.warn("There are errors while bulk saving items. {}", bulkResponse.buildFailureMessage());
					} else {
						if (!savedSchemaIdentifiers.isEmpty()) {
							rememberAlreadySavedSchemas(savedSchemaIdentifiers);
						}
					}
					bulkClient.close();
				}
			} catch (final Throwable t) {
				logger.error("Error while saving expired items", t);
			}
		}, INITIAL_DELAY_SECONDS, PROCESS_EXPIRED_DELAY_SECONDS, TimeUnit.SECONDS);
		logger.debug("Scheduled saving of expired items...");
	}

	private boolean schemaAlreadySaved(SchemaInfo si) {
		final String id = si.getId();
		return ALREADY_SAVED_SCHEMAS.contains(id);
	}

	private void rememberAlreadySavedSchemas(Set<String> schemaIdentifiers) {
		logger.debug("Remembering already saved schemas {}", schemaIdentifiers);
		if (ALREADY_SAVED_SCHEMAS.size() > MAX_ALREADY_SAVED_SCHEMAS) {
			logger.debug("Clearing already saved schemas - reached max number {}", MAX_ALREADY_SAVED_SCHEMAS);
			ALREADY_SAVED_SCHEMAS.clear();
		}
		ALREADY_SAVED_SCHEMAS.addAll(schemaIdentifiers);
	}

	@Override
	public void save(SchemaInfo schemaInfo) {
		queue.add(new PostponedWorkItem(schemaInfo, DEFAULT_DELAY_MILLIS));
	}

	@Override
	public void save(FileWithSchema file) {
		queue.add(new PostponedWorkItem(file, DEFAULT_DELAY_MILLIS));
	}

	@Override
	public void save(DataSourceInfo ds) {
		queue.add(new PostponedWorkItem(ds, DEFAULT_DELAY_MILLIS));
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public void saveLastModified(String path, long modificationTime) {
		if (path == null || path.isEmpty()) {
			throw new IllegalArgumentException("Path must not be null or empty!");
		}
		if (modificationTime < 0) {
			throw new IllegalArgumentException("Modifiation time must not be < 0");
		}
		final InternalModificationTime imt = new InternalModificationTime();
		imt.setLastModificationTime(modificationTime);
		imt.setUri(path);
		queue.add(new PostponedWorkItem(imt, DEFAULT_DELAY_MILLIS));
	}

	@Override
	public void cleanCaches() {
		ALREADY_SAVED_SCHEMAS.clear();
		deleteModificationsCache();
	}
	
	private void deleteModificationsCache() {
		final TransportClient cl = createClient(this.clusterNodes);
		final DeleteIndexResponse dir = cl.admin().indices().prepareDelete(INTERNAL_MODIFICATION_INDEX).get();
		final boolean deleteAck = dir.isAcknowledged();
		final CreateIndexResponse createIndexResponse = cl.admin().indices().prepareCreate(INTERNAL_MODIFICATION_INDEX).get();
		final boolean createAck = createIndexResponse.isAcknowledged();
		cl.close();
		if(!deleteAck){
			throw new IllegalStateException("Did not get ACK for deleting caches...");
		}
		if(!createAck){
			throw new IllegalStateException("Did not get ACK for creating caches...");
		}
	}

	static class PostponedWorkItem implements Delayed {

		private final long origin;
		private final long delay;
		private final Object workItem;

		public PostponedWorkItem(final Object workItem, final long delay) {
			this.origin = System.currentTimeMillis();
			this.workItem = workItem;
			this.delay = delay;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(delay - (System.currentTimeMillis() - origin), TimeUnit.MILLISECONDS);
		}

		@Override
		public int compareTo(Delayed delayed) {
			if (delayed == this) {
				return 0;
			}

			if (delayed instanceof PostponedWorkItem) {
				final long diff = delay - ((PostponedWorkItem) delayed).delay;
				return ((diff == 0) ? 0 : ((diff < 0) ? -1 : 1));
			}

			final long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
			return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
		}

	}

}
