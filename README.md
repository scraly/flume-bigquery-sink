# flume-bigquery-sink
An Apache Flume Sink implementation to publish data to Google BigQuery

## Configuration of Google BigQuery Sink:

>Edit log4j.xml

    <appender name="FlumeAppender" class="org.apache.flume.clients.log4jappender.Log4jAppender">
       <param name="Hostname" value="localhost"/>
       <param name="Port" value="8090"/>
    </appender>
    ...
    <logger name="flume" additivity="false">
       <level value="trace" />
       <appender-ref ref="FlumeAppender" />
    </logger>

>Edit your flume.conf:

    #list the sources, sinks and channels for the agent
    agent.sources = <your_source>
    agent.sinks =  bigquery-sink
    agent.channels = bigquery-channel
 
    # properties of <your_source>
    agent.sources.modv6-source.channels = bigquery-channel
    agent.sources.modv6-source.type = avro
    agent.sources.modv6-source.bind = localhost
    agent.sources.modv6-source.port = 8090
 
    # properties of bigquery-channel
    agent.channels.bigquery-channel.type = file
    agent.channels.bigquery-channel.checkpointDir = /data/flume-bq/checkpoint
    agent.channels.bigquery-channel.dataDirs = /data/flume-bq/data
    agent.channels.bigquery-channel.minimumRequiredSpace = 0
 
    # properties of bigquery-sink
    agent.sinks.bigquery-sink.channel = <your_channel>
    agent.sinks.bigquery-sink.type = BigQuerySink
    agent.sinks.bigquery-sink.batchSize = 100
    agent.sinks.bigquery-sink.clientId = <project_id>.apps.googleusercontent.com
    agent.sinks.bigquery-sink.clientSecret = <your_client_secret>
    agent.sinks.bigquery-sink.accessToken = <your_access_token>
    agent.sinks.bigquery-sink.refreshToken = <your_refresh_token>
    agent.sinks.bigquery-sink.dataStoreDir = /home/<your_home>/etc/<your_dir>
    agent.sinks.bigquery-sink.userId = <your_user_id>
    agent.sinks.bigquery-sink.datasetId = <your_dataset>
    agent.sinks.bigquery-sink.projectId = <your_project_id>
    
>Edit BigQueryManager class:

    private static final String PROJECT_ID = "112233445566"; // change with your google cloud projectId
    private static final String DATASET = "toto"; //change with your google bigquery dataset
    
>Change LogField and CSVUtil classes in order to tell to the BigQuery sink what is the bq table schema
