package pl.swiatowy.es.plugins;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;
import pl.swiatowy.es.rivers.KimonoRiver;

/**
 * Created: 2014-05-1310:54
 *
 * @author swiatek25
 */
public class KimonoRiverModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(River.class).to(KimonoRiver.class).asEagerSingleton();
    }
}
