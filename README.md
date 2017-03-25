# flume-bigquery-sink
An Apache Flume Sink implementation to publish data to Google BigQuery

## Configuration of Google BigQuery Sink:

flume.conf:
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