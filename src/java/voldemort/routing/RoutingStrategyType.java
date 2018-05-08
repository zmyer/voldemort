package voldemort.routing;

/**
 * An enumeration of RoutingStrategies type
 *
 *
 */
// TODO: 2018/4/26 by zmyer
public class RoutingStrategyType {

    public final static String CONSISTENT_STRATEGY = "consistent-routing";
    public final static String TO_ALL_STRATEGY = "all-routing";
    public final static String ZONE_STRATEGY = "zone-routing";
    public final static String TO_ALL_LOCAL_PREF_STRATEGY = "local-pref-all-routing";

    private final String name;

    private RoutingStrategyType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
