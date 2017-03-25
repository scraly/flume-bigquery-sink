package com.scraly.flume_bigquery_sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A sink which reads events from a channel and writes them to BigQuery.</p>
 *
 * This sink supports batch reading of events from the channel and writing them
 * to BigQuery.</p>
 * 
 * flume.conf:<pre>
 * # properties of cluster-sink
 * agent.sinks.cluster-sink.channel = <your_channel>
 * agent.sinks.cluster-sink.type = BigQuerySink
 * agent.sinks.cluster-sink.batchSize = 100
 * agent.sinks.cluster-sink.clientId = <project_id>.apps.googleusercontent.com
 * agent.sinks.cluster-sink.clientSecret = <your_client_secret>
 * agent.sinks.cluster-sink.accessToken = <your_access_token>
 * agent.sinks.cluster-sink.refreshToken = <your_refresh_token>
 * agent.sinks.cluster-sink.dataStoreDir = /home/<your_home>/etc/<your_dir>
 * agent.sinks.cluster-sink.userId = <your_user_id>
 * agent.sinks.cluster-sink.datasetId = <your_dataset>
 * agent.sinks.cluster-sink.projectId = <your_project_id>
 * </pre>
 */
public class BigQuerySink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(BigQuerySink.class);

	private BigQueryManager bqManager;
	private SinkCounter sinkCounter;
	private final CounterGroup counterGroup = new CounterGroup();

	private int batchSize = MAX_BATCH_SIZE;
	private String projectId; // Your Google Developer "Project Number"
	private String datasetId; // To be created in BigQuery console
	private String clientId; //
	private String clientSecret;
	private String accessToken;
	private String refreshToken;
	private String userId;
	private String dataStoreDir;
	
	private static final int    MAX_BATCH_SIZE = 100;
	private static final String BATCH_SIZE = "batchSize";
	private static final String PROJECT_ID = "projectId";
	private static final String DATASET_ID = "datasetId";
	private static final String CLIENT_ID = "clientId";
	private static final String CLIENT_SECRET = "clientSecret";
	private static final String ACCESS_TOKEN = "accessToken";
	private static final String REFRESH_TOKEN = "refreshToken";
	private static final String USER_ID = "userId";
	private static final String DATASTORE_DIR = "dataStoreDir";

	/**
	 * Create an {@link BigQuerySink} configured using the supplied
	 * configuration
	 */
	public BigQuerySink() {
	}

	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction txn = channel.getTransaction();

		try {
			HashMap<String, List<TableDataInsertAllRequest.Rows>> rowsByGroup =
					new HashMap<String, List<TableDataInsertAllRequest.Rows>>();
			
			txn.begin();			
			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();

				if (event == null) {
					break;
				}

				EnumMap<LogField, String> fields = new CSVUtil().parse(new String(event.getBody()));
				String tableId = fields.get(LogField.tid);
				if (tableId == null || tableId.isEmpty()) continue; // don't store lines with empty group id
				
				TableRow tr = new TableRow();
				String tableName = tableId;
				for (LogField lf : fields.keySet()) {
					switch (lf) {
					case lv://log version
						break;
					default:
						// set null value if empty (or already null)
						String val = fields.get(lf) != null && fields.get(lf).isEmpty() ? null : fields.get(lf);
						tr.set(lf.toString(), val);
						break;
					}
				}
				String insertId = fields.get(LogField.cdt)+fields.get(LogField.id);
				tr.set("insertId", insertId);
				TableDataInsertAllRequest.Rows row = new TableDataInsertAllRequest.Rows()
					.setInsertId(insertId)
					.setJson(tr);

				List<TableDataInsertAllRequest.Rows> rowList = rowsByGroup.get(tableName);
				if (rowList == null) rowList = new ArrayList<TableDataInsertAllRequest.Rows>();

				rowList.add(row);
				rowsByGroup.put(tableName, rowList);
			}

			int size = 0;
			for (List<TableDataInsertAllRequest.Rows> rowList : rowsByGroup.values()) {
				size += rowList.size();
			}
			if (size > 0) {
				logger.info("processing nb rows: " + size);
			}
			if (size <= 0) {
				sinkCounter.incrementBatchEmptyCount();
				counterGroup.incrementAndGet("channel.underflow");
				status = Status.BACKOFF;
			} else {
				if (size < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}

				sinkCounter.addToEventDrainAttemptCount(size);

				for (String tableName : rowsByGroup.keySet()) {
					boolean error = true;
					List<TableDataInsertAllRequest.Rows> rowList = rowsByGroup.get(tableName);
					do {
						TableDataInsertAllResponse response = bqManager.insertRowsInTable(tableName, rowList);
						List<InsertErrors> errors = response.getInsertErrors();
						if (errors != null) {
							for (InsertErrors err : errors) {
								if (err.getErrors() != null) {
									Long idx = err.getIndex();
									// log row in error
									rowList.set(idx.intValue(), null);
								}
							}
							List<TableDataInsertAllRequest.Rows> rowList2 = new ArrayList<TableDataInsertAllRequest.Rows>();
							for (Rows r : rowList) {
								if (r != null) {
									rowList2.add(r);
								}
							}
							rowList.clear();
							rowList.addAll(rowList2);
							logger.warn("Some rows were in error, trying again without them. Error was: " + errors);
						} else {
							error = false;
						}
					} while (error && !rowList.isEmpty());
				}
			}
			txn.commit();
			sinkCounter.addToEventDrainSuccessCount(size);
			counterGroup.incrementAndGet("transaction.success");
		} catch (Throwable ex) {
			try {
				txn.rollback();
				counterGroup.incrementAndGet("transaction.rollback");
			} catch (Exception ex2) {
				logger.error("Exception in rollback. Rollback might not have been successful.",
						ex2);
			}

			if (ex instanceof Error || ex instanceof RuntimeException) {
				logger.error("Failed to commit transaction. Transaction rolled back.", ex);
				Throwables.propagate(ex);
			} else {
				logger.error("Failed to commit transaction. Transaction rolled back.", ex);
				throw new EventDeliveryException(
						"Failed to commit transaction. Transaction rolled back.", ex);
			}
		} finally {
			txn.close();
		}
		return status;
	}

	public void configure(Context context) {
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

		if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
			this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
			if (this.batchSize > BigQuerySink.MAX_BATCH_SIZE) this.batchSize = BigQuerySink.MAX_BATCH_SIZE;
		}

		if (StringUtils.isNotBlank(context.getString(PROJECT_ID))) {
			this.projectId = context.getString(PROJECT_ID);
		}

		if (StringUtils.isNotBlank(context.getString(DATASET_ID))) {
			this.datasetId = context.getString(DATASET_ID);
		}

		if (StringUtils.isNotBlank(context.getString(CLIENT_ID))) {
			this.clientId = context.getString(CLIENT_ID);
		}

		if (StringUtils.isNotBlank(context.getString(CLIENT_SECRET))) {
			this.clientSecret = context.getString(CLIENT_SECRET);
		}

		if (StringUtils.isNotBlank(context.getString(ACCESS_TOKEN))) {
			this.accessToken = context.getString(ACCESS_TOKEN);
		}

		if (StringUtils.isNotBlank(context.getString(REFRESH_TOKEN))) {
			this.refreshToken = context.getString(REFRESH_TOKEN);
		}

		if (StringUtils.isNotBlank(context.getString(USER_ID))) {
			this.userId = context.getString(USER_ID);
		}

		if (StringUtils.isNotBlank(context.getString(DATASTORE_DIR))) {
			this.dataStoreDir = context.getString(DATASTORE_DIR);
		}

		Preconditions.checkState(StringUtils.isNotBlank(projectId),
				"Missing Param:" + PROJECT_ID);
		Preconditions.checkState(StringUtils.isNotBlank(datasetId),
				"Missing Param:" + DATASET_ID);
		Preconditions.checkState(StringUtils.isNotBlank(clientId),
				"Missing Param:" + CLIENT_ID);
		Preconditions.checkState(StringUtils.isNotBlank(clientSecret),
				"Missing Param:" + CLIENT_SECRET);
		Preconditions.checkState(StringUtils.isNotBlank(accessToken),
				"Missing Param:" + ACCESS_TOKEN);
		Preconditions.checkState(StringUtils.isNotBlank(refreshToken),
				"Missing Param:" + REFRESH_TOKEN);
		Preconditions.checkState(StringUtils.isNotBlank(userId),
				"Missing Param:" + USER_ID);
		Preconditions.checkState(StringUtils.isNotBlank(dataStoreDir),
				"Missing Param:" + DATASTORE_DIR);
		Preconditions.checkState(batchSize >= 1, BATCH_SIZE
				+ " must be greater than 0");
	}

	@Override
	public void start() {
		logger.info("BigQuery sink {} started");
		sinkCounter.start();
		try {
			openConnection();
		} catch (Exception ex) {
			sinkCounter.incrementConnectionFailedCount();
			closeConnection();
			logger.error("Failed starting BigQuery sink", ex);
		}

		super.start();
	}

	@Override
	public void stop() {
		logger.info("BigQuery sink {} stopping");
		closeConnection();

		sinkCounter.stop();
		super.stop();
	}

	private void openConnection() throws IOException {
		logger.info("BigQuery openConnection");
		bqManager = new BigQueryManager(projectId, datasetId, clientId, clientSecret,
	            accessToken, refreshToken, userId, dataStoreDir);
		sinkCounter.incrementConnectionCreatedCount();
	}

	private void closeConnection() {
		if (bqManager != null) bqManager.close();
		sinkCounter.incrementConnectionClosedCount();
	}
}
