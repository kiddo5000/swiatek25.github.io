package pl.swiatowy.es.rivers;

import com.google.common.base.Throwables;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import pl.swiatowy.es.client.ClientFactory;
import pl.swiatowy.es.client.EsClient;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class KimonoRiver extends AbstractRiverComponent implements River {

    private URL url;
    private String resultsCollectionName;
    private String indexName;

    private final EsClient client;

    @Inject
    public KimonoRiver(RiverSettings settings, Client client) {
        super(new RiverName("REST API", "Kimono API river"), settings);
        this.client = ClientFactory.enhance(client);
        this.resultsCollectionName = (String) settings.settings().getOrDefault("resultsCollectionName", "movies");
        this.indexName = (String) settings.settings().getOrDefault("indexName", "movies");
        try {
            this.url = new URL((String) settings.settings().getOrDefault("url", "http://www.kimonolabs.com/api/8216m0qu?apikey=840e981ea4e9f1ec7e0d559e850e31f0"));
        } catch (MalformedURLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void start() {
        logger.info("{} started", this.getClass().getName());

        client.forceCreateIndex(indexName);

        try (InputStream is = url.openStream(); JsonReader rdr = Json.createReader(is)) {
            JsonObject obj = rdr.readObject();
            JsonArray movies = obj.getJsonObject("results").getJsonArray(resultsCollectionName);
            BulkRequestBuilder requestBuilder = client.prepareBulk();

            movies.getValuesAs(JsonObject.class).forEach(
                    movie -> requestBuilder.add(client.prepareIndex(indexName, "item").setSource(getAsString(movie)))
            );

            BulkResponse bulkResponse = requestBuilder.execute().actionGet();
            logger.info("Indexing took {}", bulkResponse.getTook());
            if (bulkResponse.hasFailures()) {
                logger.warn("Some documents could not be indexed bcs of the following error : {}", bulkResponse.buildFailureMessage());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {

    }

    private String getAsString(JsonObject object) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Json.createWriter(out).write(object);
            return out.toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
