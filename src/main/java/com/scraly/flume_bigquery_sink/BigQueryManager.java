package com.scraly.flume_bigquery_sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class BigQueryManager {

	private static final Logger logger = LoggerFactory.getLogger(BigQueryManager.class);

    private static final String TYPE_STRING = "STRING";
	private static final String PROJECT_ID = "112233445566"; // change with your google cloud projectId
    private static final String DATASET = "toto"; //change with your google bigquery dataset
    private static final int SLEEP_MILLIS = 5000;

    private static final long ONE_YEAR_RETENTION = 367*24*60*60*1000L;

    private String projectId; // Your Google Developer "Project Number"
    private String datasetId; // To be created in BigQuery console
    private GoogleHelper googleHelper;
    private Bigquery bigquery;

    public BigQueryManager(String projectId, String datasetId) throws IOException {
        super();
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.googleHelper = new GoogleHelper();
        this.bigquery = new Bigquery.Builder(GoogleHelper.HTTP_TRANSPORT, GoogleHelper.JSON_FACTORY, this.googleHelper.getCredential())
            .setHttpRequestInitializer(this.googleHelper.getCredential())
            .setApplicationName("BigQuerySinkForFlume")
            .build();           
    }

    public BigQueryManager() throws IOException {
        this(PROJECT_ID, DATASET);

    }

    public BigQueryManager(String projectId, String datasetId,
            String clientId, String clientSecret, String accessToken,
            String refreshToken, String userId, String dataStoreDir) throws IOException {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.googleHelper = new GoogleHelper(clientId, clientSecret, accessToken,
                refreshToken, userId, dataStoreDir);
        this.bigquery = new Bigquery.Builder(GoogleHelper.HTTP_TRANSPORT, GoogleHelper.JSON_FACTORY, 
        		this.googleHelper.getCredential())
            .setHttpRequestInitializer(this.googleHelper.getCredential())
            .setApplicationName(DATASET)
            .build();
    }

    public Bigquery getBigquery() {
        return this.bigquery;
    }

    public List<Tables> getTablesList() throws IOException {
        return getBigquery().tables()
                .list(this.projectId, this.datasetId).execute().getTables();
    }

    public boolean checkTableExists(String tableId) {
        try {
            getBigquery().tables()
            .get(this.projectId, this.datasetId, tableId).execute();
            return true;
        } catch (IOException e) { // table doesn't exists
            return false;
        }
    }

    public boolean checkTableExists(List<Tables> tables, String tableId) throws IOException {
        boolean exists = false;
        if (tables != null) {
            for (Tables t : tables) {
                if (t.getTableReference().getTableId().equals(tableId)) {
                    exists = true;
                    break;
                }
            }
        }
        return exists;
    }

    public Table createTable(String tableId, String[] schemaDef) throws IOException {
        List<TableFieldSchema> sfList = new ArrayList<TableFieldSchema>();
        for (String name : schemaDef) {
            String type = TYPE_STRING;
            if (name.contains(":")) {
                String[] nd = name.split(":");
                name = nd[0];
                type = nd[1];
            }
            sfList.add(new TableFieldSchema().setName(name).setType(type));
        } 
        TableSchema schema = new TableSchema().setFields(sfList);
        TableReference tableReference = new TableReference()
        .setProjectId(this.projectId)
        .setDatasetId(this.datasetId)
        .setTableId(tableId);

        long expirationTime = System.currentTimeMillis()+ONE_YEAR_RETENTION;

        Table table = new Table()
        .setTableReference(tableReference)
        .setExpirationTime(expirationTime)
        .setSchema(schema);
        return getBigquery().tables()
                .insert(this.projectId, this.datasetId, table).execute();
    }

    public TableDataInsertAllResponse insertRowsInTable(String tableName, 
            List<TableDataInsertAllRequest.Rows> rowList) throws IOException {
        return getBigquery().tabledata()
                .insertAll(this.projectId, this.datasetId, tableName, 
                        new TableDataInsertAllRequest().setRows(rowList))
                        .execute();
    }

    public QueryResponse queryTable(String query) throws IOException {
        return queryTable(query, null, null);
    }

    public QueryResponse queryTable(String query, Long maxRes, Long timeout) throws IOException {
        QueryRequest request = new QueryRequest();
        if (maxRes != null) {
            request.setMaxResults(maxRes);
        }
        if (timeout != null) {
            request.setTimeoutMs(timeout);
        }
        request.setQuery(query);
        request.setUseQueryCache(true);
        QueryResponse queryResponse = getBigquery().jobs()
                .query(this.projectId, request).execute();
        return queryResponse;

    }

    public GetQueryResultsResponse getQueryResults(String jobId, Long maxRes, Long timeout) throws IOException {
        GetQueryResults resultsReq = getBigquery().jobs().getQueryResults(projectId, jobId);
        if (timeout != null)
            resultsReq.setTimeoutMs(timeout);
        if (maxRes != null)
            resultsReq.setMaxResults(maxRes);
        GetQueryResultsResponse response = resultsReq.execute();
        while (!response.getJobComplete()) {
            try {
                Thread.sleep(SLEEP_MILLIS);
            } catch (InterruptedException e) {
            }
            response = resultsReq.execute();
        }
        return response;
    }

    public void close() {

    }

    public List<TableRow> runQueryRequest(String query) throws IOException {
        List<TableRow> res = new ArrayList<TableRow>();
        QueryRequest queryRequest = new QueryRequest().setQuery(query).setUseQueryCache(true);
        QueryResponse queryResponse = getBigquery().jobs().query(this.projectId, queryRequest).execute();
        if (queryResponse.getJobComplete()) {
            res.addAll(queryResponse.getRows());
            if (null == queryResponse.getPageToken()) {
                return res;
            }
        }
        return getQueryResults(queryResponse.getJobReference(), res);
    }

    public List<TableRow> getQueryResults(JobReference jref) throws IOException {
        return getQueryResults(jref, null);
    }

    private List<TableRow> getQueryResults(JobReference jref, List<TableRow> res) throws IOException {
        logger.debug("getQueryResults: jref= " + jref);

        // This loop polls until results are present, then loops over result pages.
        String pageToken = null;
        if (res == null) {
            res = new ArrayList<TableRow>();
        }
        while (true) {
            GetQueryResultsResponse queryResults = getBigquery().jobs().getQueryResults(projectId, jref.getJobId())
                    .setPageToken(pageToken).execute();
            if (queryResults.getJobComplete()) {
                if(queryResults.getRows() != null) {
                    res.addAll(queryResults.getRows());
                    pageToken = queryResults.getPageToken();
                    if (null == pageToken) {
                        return res;
                    }
                } else {
                    return res;
                }
            }
        }       
    }

    public Job startAsyncQuery(String query, String jobId)
            throws IOException {
        logger.debug("startAsyncQuery: query=" + query + ", jobId=" + jobId);

        JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query).setUseQueryCache(true);
        JobConfiguration config = new JobConfiguration().setQuery(queryConfig);
        Job job = new Job().setId(jobId).setConfiguration(config);
        logger.debug("will insert jobs in queue");
        Job queuedJob = getBigquery().jobs().insert(this.projectId, job).execute();

        logger.debug("end of startAsyncQuery");
        return queuedJob;
    }

    public Job getJob(String query, String jobId) throws IOException {
        logger.debug("getJob: query=" + query + ", jobId=" + jobId);

        JobConfigurationQuery queryConfig = new JobConfigurationQuery().setQuery(query).setUseQueryCache(true);
        JobConfiguration config = new JobConfiguration().setQuery(queryConfig);
        Job job = new Job().setId(jobId).setConfiguration(config);
        logger.debug("new job created: " + job + " - jobId: " + jobId);
        logger.debug("end of queryJob");
        return job;
    }

    public void executingJobsInRequestBatching(ArrayList<Job> jobs, JsonBatchCallback<Job> callback) throws IOException {
        logger.debug("Add BigQuery using Batch");

        BatchRequest batch = getBigquery().batch();
        for (Job job : jobs) {
            getBigquery().jobs().insert(this.projectId, job).queue(batch, callback);
        }

        logger.debug("executing requestbatching");

        batch.execute();
    }

    /**
     * 
     * @param job
     * @return -1 if error, 0 if not finished, 1 if done
     * @throws IOException 
     */
    public Job pollJob(JobReference jref) throws IOException {
        return getBigquery().jobs()
                .get(jref.getProjectId(), jref.getJobId())
                .execute();
    }
}
