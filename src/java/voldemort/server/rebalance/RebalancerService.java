package voldemort.server.rebalance;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationService;
import voldemort.store.metadata.MetadataStore;

/**
 */
// TODO: 2018/4/26 by zmyer
@JmxManaged(description = "Rebalancer service to help with rebalancing")
public class RebalancerService extends AbstractService {
    //调度服务
    private final SchedulerService schedulerService;
    //平衡器
    private final Rebalancer rebalancer;

    // TODO: 2018/4/26 by zmyer
    public RebalancerService(StoreRepository storeRepository,
            MetadataStore metadataStore,
            VoldemortConfig voldemortConfig,
            AsyncOperationService asyncService,
            SchedulerService service) {
        super(ServiceType.REBALANCE);
        schedulerService = service;
        rebalancer = new Rebalancer(storeRepository, metadataStore, voldemortConfig, asyncService);
    }

    // TODO: 2018/4/26 by zmyer
    @Override
    protected void startInner() {
        rebalancer.start();
        schedulerService.scheduleNow(rebalancer);
    }

    // TODO: 2018/4/26 by zmyer
    @Override
    protected void stopInner() {
        rebalancer.stop();
    }

    // TODO: 2018/4/26 by zmyer
    public Rebalancer getRebalancer() {
        return rebalancer;
    }
}
