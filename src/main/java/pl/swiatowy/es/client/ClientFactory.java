package pl.swiatowy.es.client;

import com.google.common.base.Preconditions;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created: 2014-05-1214:37
 *
 * @author swiatek25
 */
public class ClientFactory {
    private static final Logger log = Logger.getLogger(ClientFactory.class.getName());
    private final String hostName;
    private final int portNumber;

    public ClientFactory(String hostName, int portNumber) {
        this.hostName = hostName;
        this.portNumber = portNumber;
    }

    public ClientFactory() {
        this("localhost", 9300);
    }

    public EsClient getClient() {
        log.log(Level.INFO, "Getting client...");
        TransportClient transportClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress(hostName, portNumber));
        log.log(Level.INFO, "Checking cluster health...");

        ClusterHealthResponse clusterIndexHealths = transportClient.admin().cluster().health(new ClusterHealthRequest()).actionGet(2, TimeUnit.MINUTES);

        Preconditions.checkArgument(
                clusterIndexHealths.getStatus().value() < ClusterHealthStatus.RED.value(),//not red
                "Returned client with health status " + clusterIndexHealths.getStatus());

        log.log(Level.INFO, "All done.");
        return Reflection.newProxy(EsClient.class, new EsClientInvocationHandler(transportClient));
    }

    public static EsClient enhance(Client client) {
        return Reflection.newProxy(EsClient.class, new EsClientInvocationHandler(client));
    }

    private static class EsClientInvocationHandler extends AbstractInvocationHandler {

        private final Client client;
        private final ClientInstance clientInstance;

        public final class ClientInstance extends TransportClient implements EsClient {
            @Override
            public Client getClientSource() {
                return client;
            }
        }

        private EsClientInvocationHandler(Client client) {
            this.client = client;
            this.clientInstance = new ClientInstance();
        }

        @Override
        protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.isAnnotationPresent(Delegate.class)) {
                return client;
            }
            if (method.isDefault()) {
                return method.invoke(clientInstance, args);
            }
            return method.invoke(client, args);
        }
    }

}
