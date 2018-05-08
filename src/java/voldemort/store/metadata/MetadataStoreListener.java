package voldemort.store.metadata;

import voldemort.routing.RoutingStrategy;
import voldemort.store.StoreDefinition;

// TODO: 2018/4/26 by zmyer
public interface MetadataStoreListener {

    void updateRoutingStrategy(RoutingStrategy routingStrategyMap);

    void updateStoreDefinition(StoreDefinition storeDef);
}
