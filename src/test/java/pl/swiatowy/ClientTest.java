package pl.swiatowy;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pl.swiatowy.es.client.ClientFactory;
import pl.swiatowy.es.client.EsClient;

public class ClientTest {

    private EsClient client;

    @Before
    public void createClient() {
        client = new ClientFactory().getClient();
    }

    @After
    public void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void checkNodesCount_shouldBeAtLeastOneActive() {
        Assert.assertTrue(client.getNodesCount() > 0);
    }

    @Test
    public void printClusterNodesInfo() {
        client.printNodesInfo();
    }

    @Test
    public void manipulateIndex_shouldReturnPresentState() {
        client.createIndex("test");
        Assert.assertTrue(client.isIndexPresent("test"));
        client.removeIndex("test");
        Assert.assertFalse(client.isIndexPresent("test"));
    }
}