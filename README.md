# Gcs Streaming file output plugin for Embulk

Google Cloud Storage (GCS) Streaming file output plugin for Embulk.  
No use local storage when upload to GCS. Using [Streming transters](https://cloud.google.com/storage/docs/streaming).

## Overview

* **Plugin type**: file output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **project_id**: Google Cloud Platform Project ID (string, required)
- **bucket**: Google Cloud Storage bucket name (string, required)
- **path_prefix**: Prefix of output keys (string, required)
- **file_ext**: Extension of output file (string, required)
- **sequence_format**: Format of the sequence number of the output files (string, default value is `.%03d.%02d.`)
- **content_type**: content type of output file (string, optional, default value is `application/octet-stream`)
- **json_keyfile** full path of json_key (string, optional)

### Authentication

If you do not set service account `json_keyfile`, this plugin uses a library called Application Default Credentials (ADC) to automatically find your service account credentials.

1. If the environment variable `GOOGLE_APPLICATION_CREDENTIALS` is set, ADC uses the service account file that the variable points to.
1. If the environment variable `GOOGLE_APPLICATION_CREDENTIALS` isn't set, ADC uses the default service account that GCP services provide.

Following [Authenticating as a service account](https://cloud.google.com/docs/authentication/production).

## Example

```yaml
  type: gcs_streaming
  project_id: project
  bucket: budket
  path_prefix: path/to/file
  file_ext: csv
  formatter:
    type: csv
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
