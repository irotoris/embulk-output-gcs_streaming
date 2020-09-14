package org.embulk.output.gcs_streaming;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeNotNull;

import java.util.Optional;

public class TestGcsClient
{
    private static String gcpProjectId;
    private static Optional<String> gcpJsonKeyfile;
    /*
     * This test case requires environment variables
     *   GCP_PROJECT_ID
     *   GCP_JSON_KEYFILE
     * And prepare gcloud authentication ADC, following command.
     *   $ gcloud auth application-default login
     */
    @BeforeClass
    public static void initializeConstant()
    {
        gcpProjectId = System.getenv("GCP_PROJECT_ID");
        gcpJsonKeyfile = Optional.of(System.getenv("GCP_JSON_KEYFILE"));
        assumeNotNull(gcpJsonKeyfile, gcpProjectId);
        // skip test cases, if environment variables are not set.
    }

    @Test
    public void testGetStorageSuccess() throws RuntimeException
    {
        Optional<String> empty = Optional.empty();
        GcsClient client = new GcsClient(gcpProjectId, empty);
    }

    @Test
    public void testGetStorageSuccessFromJsonKeyfile() throws RuntimeException
    {
        GcsClient client = new GcsClient(gcpProjectId, gcpJsonKeyfile);
    }

    @Test(expected = RuntimeException.class)
    public void testGetStorageFailFromJsonKeyfile() throws RuntimeException
    {
        Optional<String> notFoundJsonKeyfile = Optional.of("/path/to/key.json");
        GcsClient client = new GcsClient(gcpProjectId, notFoundJsonKeyfile);
        assertEquals(1, 2);
    }
}
