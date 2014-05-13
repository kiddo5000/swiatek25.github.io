package pl.swiatowy.es.rivers;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.river.RiverSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.swiatowy.es.client.ClientFactory;
import pl.swiatowy.es.client.EsClient;

import java.util.Collections;

public class KimonoRiverTest {

    private EsClient client;

    @Before
    public void createClient() {
        client = new ClientFactory().getClient();
    }

    @After
    public void closeClientAndIndex() {
        client.removeIndex("movies");
        client.close();
    }

    @Test
    public void startIndexing() {
        RiverSettings settings = new RiverSettings(ImmutableSettings.EMPTY, Collections.emptyMap());
        new KimonoRiver(settings, client).start();
    }

}