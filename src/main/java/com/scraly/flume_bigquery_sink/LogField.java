package com.scraly.flume_bigquery_sink;

public enum LogField {
	id, //ID of the object that you want to store
    cdt, // For example, your first field on BigQuery table: creation date and time
    txt, // For example, the second field of your bq table: the content of the message that you store
    tid, // The table name/id where you want to store the data
    lv // Log Version (starts at 'v1')
}