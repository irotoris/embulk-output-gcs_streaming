package org.embulk.output.gcs_streaming;

import com.google.auth.oauth2.ServiceAccountCredentials;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;

public class GcsClient
{
    public Storage storage;
    final String projectId;
    final Optional<String> jsonKeyfile;

    public GcsClient(String projectId, Optional<String> jsonKeyfile)
    {
        this.projectId = projectId;
        this.jsonKeyfile = jsonKeyfile;
        if (this.jsonKeyfile.isPresent()) {
            try {
                String key = jsonKeyfile.get();
                this.storage = getClientWithJsonKey(this.projectId, key);
            }
            catch (IOException | StorageException ex) {
                throw new RuntimeException(ex);
            }
        }
        else {
            try {
                this.storage = getClient(this.projectId);
            }
            catch (StorageException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static Storage getClientWithJsonKey(String projectId, String key) throws IOException, StorageException
    {
        return StorageOptions.newBuilder()
                .setProjectId(projectId)
                .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(key)))
                .build()
                .getService();
    }

    private static Storage getClient(String projectId) throws StorageException
    {
        return StorageOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
    }
}
