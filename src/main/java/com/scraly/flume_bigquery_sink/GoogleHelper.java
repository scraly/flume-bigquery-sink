package com.scraly.flume_bigquery_sink;

import java.io.File;
import java.io.IOException;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.DataStoreCredentialRefreshListener;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;

public class GoogleHelper {

	private static final String DEFAULT_DATASTORE_DIR = "/home/air/etc/moderation";
	private static final String DEFAULT_USERID = "toto@gmail.com"; //change with your userId
	private static final String DEFAULT_CLIENT_SECRET = "your_cient_secret";
	private static final String DEFAULT_CLIENT_ID = "your_client_id";

    // From javadoc http://javadoc.google-http-java-client.googlecode.com/hg/1.10.2-beta/com/google/api/client/http/HttpTransport.html
    // "Implementation is thread-safe, and sub-classes must be thread-safe. For maximum efficiency, 
    // applications should use a single globally-shared instance of the HTTP transport."
    public static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
    // Implementation is thread-safe, and sub-classes must be thread-safe. For maximum efficiency, 
    // applications should use a single globally-shared instance of the JSON factory.
    public static final JsonFactory JSON_FACTORY = new JacksonFactory();
    
    private String clientId;
    private String clientSecret;
    private String accessToken;
    private String refreshToken;
    private String userId; 
    private String dataStoreDir;
    private Credential credential;

    public GoogleHelper(String clientId, String clientSecret, 
    		String accessToken, 
    		String refreshToken, String userId,
            String dataStoreDir) throws IOException {
        super();
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.userId = userId;
        this.dataStoreDir = dataStoreDir;
        DataStoreFactory credentialStore = new FileDataStoreFactory(new File(this.dataStoreDir));
        this.credential = new GoogleCredential.Builder()
	        .setTransport(HTTP_TRANSPORT)
	        .setJsonFactory(JSON_FACTORY)
	        .setClientSecrets(this.clientId, this.clientSecret)
	        .addRefreshListener(new DataStoreCredentialRefreshListener(this.userId, credentialStore))
	        .build()
	        .setAccessToken(this.accessToken)
	        .setRefreshToken(this.refreshToken);
    }

    public GoogleHelper() throws IOException {
        this(DEFAULT_CLIENT_ID, DEFAULT_CLIENT_SECRET,
                "ya29.123456789123456789123456789123456798",
                "1/HNA-545154546732132135435354", DEFAULT_USERID,
                DEFAULT_DATASTORE_DIR);
    }

    public GoogleHelper(String accessToken, String refreshToken, String userIdExt) throws IOException {
        this(DEFAULT_CLIENT_ID,  DEFAULT_CLIENT_SECRET,
                accessToken,
                refreshToken, DEFAULT_USERID+"/"+userIdExt,
                DEFAULT_DATASTORE_DIR);    	
    }
    
    public Credential getCredential() {
        return this.credential;
    }

}
