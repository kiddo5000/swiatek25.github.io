package pl.swiatowy;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created: 2014-05-0915:48
 *
 * @author swiatek25
 */
public class Test {
    private static final Logger log = Logger.getLogger(Test.class.getName());

    private final static Client client;

    static {
        client = assertHealthyCluster();
    }

    public static void main(String[] args) {
        createIndex("movies");
        client.close();
    }

    private static void createIndex(String index) {
        try {
            IndicesAdminClient indicesAdminClient = client.admin().indices();
            if (indicesAdminClient.exists(new IndicesExistsRequest(index)).actionGet().isExists()) {
                log.log(Level.INFO, "Index already exists. Skipping creation");
            } else {
                indicesAdminClient.create(new CreateIndexRequest(index)).get(20, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.log(Level.SEVERE, e, e::getMessage);
            throw Throwables.propagate(e);
        }
    }

    private static Client assertHealthyCluster() {
        log.log(Level.INFO, "Getting client...");
        TransportClient transportClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        log.log(Level.INFO, "Checking cluster health...");

        ClusterHealthResponse clusterIndexHealths = transportClient.admin().cluster().health(new ClusterHealthRequest()).actionGet(2, TimeUnit.MINUTES);

        Preconditions.checkArgument(
                clusterIndexHealths.getStatus().value() < ClusterHealthStatus.RED.value(),//not red
                "Returned client with health status " + clusterIndexHealths.getStatus());

        log.log(Level.INFO, "All done.");

        return transportClient;
    }

}
