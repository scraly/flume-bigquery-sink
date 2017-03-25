# flume-bigquery-sink
An Apache Flume Sink implementation to publish data to Google BigQuery

## Configuration of Google BigQuery Sink:

>Edit your flume.conf:

    # properties of cluster-sink
    agent.sinks.cluster-sink.channel = <your_channel>
    agent.sinks.cluster-sink.type = BigQuerySink
    agent.sinks.cluster-sink.batchSize = 100
    agent.sinks.cluster-sink.clientId = <project_id>.apps.googleusercontent.com
    agent.sinks.cluster-sink.clientSecret = <your_client_secret>
    agent.sinks.cluster-sink.accessToken = <your_access_token>
    agent.sinks.cluster-sink.refreshToken = <your_refresh_token>
    agent.sinks.cluster-sink.dataStoreDir = /home/<your_home>/etc/<your_dir>
    agent.sinks.cluster-sink.userId = <your_user_id>
    agent.sinks.cluster-sink.datasetId = <your_dataset>
    agent.sinks.cluster-sink.projectId = <your_project_id>
    
>Edit BigQueryManager class:

    private static final String PROJECT_ID = "112233445566"; // change with your google cloud projectId
    private static final String DATASET = "toto"; //change with your google bigquery dataset
    
>Change LogField and CSVUtil classes in order to tell to the BigQuery sink what is the bq table schema