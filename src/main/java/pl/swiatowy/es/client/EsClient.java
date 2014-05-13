package pl.swiatowy.es.client;

import com.google.common.base.Throwables;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.client.Client;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;

/**
 * Created: 2014-05-1214:40
 *
 * @author swiatek25
 */
public interface EsClient extends Client {
    static final Logger log = Logger.getLogger(ClientFactory.class.getName());

    @Delegate
    public Client getClientSource();

    default public void printNodesInfo() {
        Map<String, NodeInfo> nodesMap = getClientSource().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodesMap();
        for (Map.Entry<String, NodeInfo> infoEntry : nodesMap.entrySet()) {
            log.log(Level.INFO, format("\n- Node id : %s \n- Transport info : %s\n", infoEntry.getKey(), infoEntry.getValue().getTransport().getAddress()));
        }
        log.log(Level.INFO, "Total nodes count : " + nodesMap.size());
    }

    default public int getNodesCount() {
        return getClientSource().admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodesMap().size();
    }

    default public void createIndex(String index) {
        log.log(Level.INFO, "Creating index " + index);
        try {
            if (isIndexPresent(index)) {
                log.log(Level.INFO, "Index already exists. Skipping creation");
            } else {
                getClientSource().admin().indices().create(new CreateIndexRequest(index)).get(20, TimeUnit.SECONDS);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.log(Level.SEVERE, e, e::getMessage);
            throw Throwables.propagate(e);
        }
    }

    default public void forceCreateIndex(String index) {
        removeIndex(index);
        createIndex(index);
    }

    default public boolean removeIndex(String index) {
        log.log(Level.INFO, "Removing index " + index);
        return isIndexPresent(index)
                && getClientSource().admin().indices().delete(new DeleteIndexRequest(index)).actionGet().isAcknowledged();
    }

    default public boolean isIndexPresent(String index) {
        return getClientSource().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
    }
}
