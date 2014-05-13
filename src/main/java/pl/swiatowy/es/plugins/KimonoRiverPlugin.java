package pl.swiatowy.es.plugins;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;

/**
 * Created: 2014-05-1310:45
 *
 * @author swiatek25
 */
public class KimonoRiverPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "river-kimono";
    }

    @Override
    public String description() {
        return "River that allows consuming Kimono APIs";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("kimono", KimonoRiverModule.class);
    }
}
