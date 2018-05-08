/*
 * Copyright 2008-2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.server.protocol.admin;

import com.google.common.collect.ImmutableSet;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Asynchronous job scheduler for admin service operations.
 *
 * TODO: requesting a unique id, then creating an operation with that id seems
 * like a bad API design.
 *
 */
// TODO: 2018/3/23 by zmyer
@JmxManaged(description = "Asynchronous operation execution")
public class AsyncOperationService extends AbstractService {
    //异步集合
    private final Map<Integer, AsyncOperation> operations;
    //统计最近一次操作id
    private final AtomicInteger lastOperationId = new AtomicInteger(0);
    //调度执行器
    private final SchedulerService scheduler;

    //日志对象
    private final static Logger logger = Logger.getLogger(AsyncOperationService.class);

    // TODO: 2018/4/4 by zmyer
    public AsyncOperationService(SchedulerService scheduler, int cacheSize) {
        super(ServiceType.ASYNC_SCHEDULER);
        operations = Collections.synchronizedMap(new AsyncOperationCache(cacheSize));
        this.scheduler = scheduler;
    }

    /**
     * Submit a operations. Throw a run time exception if the operations is
     * already submitted
     *
     * @param operation The asynchronous operations to submit
     * @param requestId Id of the request
     */
    // TODO: 2018/4/4 by zmyer
    public synchronized void submitOperation(int requestId, AsyncOperation operation) {
        if (this.operations.containsKey(requestId)) {
            throw new VoldemortException("Request " + requestId
                    + " already submitted to the system");
        }

        //将待执行的操作，插入到集合中
        this.operations.put(requestId, operation);
        //开始调度任务
        scheduler.scheduleNow(operation);
        logger.debug("Handling async operation " + requestId);
    }

    /**
     * Check if the an operation is done or not. 
     *
     * @param requestId Id of the request
     * @param remove Whether remove the request out of the list if it is done.
     * @return True if request is complete, false otherwise
     */
    // TODO: 2018/4/4 by zmyer
    public synchronized boolean isComplete(int requestId, boolean remove) {
        if (!operations.containsKey(requestId)) {
            throw new VoldemortException("No operation with id " + requestId + " found");
        }

        if (operations.get(requestId).getStatus().isComplete()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Operation complete " + requestId);
            }

            if (remove) {
                //如果任务已经执行完毕，则可以从集合中删除
                operations.remove(requestId);
            }
            return true;
        }
        return false;
    }

    /**
     * A default caller. remove operations ID out of the list when it is done.
     * @param requestId Id of the request
     */
    // TODO: 2018/4/4 by zmyer
    public synchronized boolean isComplete(int requestId) {
        return isComplete(requestId, true);
    }

    // Wrap getOperationStatus to avoid throwing exception over JMX
    @JmxOperation(description = "Retrieve operation status")
    public String getStatus(int id) {
        try {
            return getOperationStatus(id).toString();
        } catch (VoldemortException e) {
            return "No operation with id " + id + " found";
        }
    }

    // TODO: 2018/4/4 by zmyer
    public List<Integer> getMatchingAsyncOperationList(String jobDescPattern, boolean showCompleted) {
        //获取指定状态的任务集合
        List<Integer> operationIds = getAsyncOperationList(showCompleted);
        List<Integer> matchingOperationIds = new ArrayList<Integer>(operationIds.size());
        for (Integer operationId : operationIds) {
            //读取每个异步执行的任务对象
            AsyncOperation operation = operations.get(operationId);
            //读取运行状态信息
            String operationDescription = operation.getStatus().getDescription();
            if (operationDescription != null && operationDescription.indexOf(jobDescPattern) != -1) {
                //将满足状态信息的任务插入到集合中
                matchingOperationIds.add(operationId);
            }
        }
        //返回结果
        return matchingOperationIds;
    }

    // TODO: 2018/4/4 by zmyer
    @JmxOperation(description = "Retrieve all operations")
    public String getAllAsyncOperations() {
        String result;
        try {
            result = operations.toString();
        } catch (Exception e) {
            result = e.getMessage();
        }
        return result;
    }

    /**
     * Get list of asynchronous operations on this node. By default, only the
     * pending operations are returned.
     *
     * @param showCompleted Show completed operations
     * @return A list of operation ids.
     */
    // TODO: 2018/4/4 by zmyer
    public List<Integer> getAsyncOperationList(boolean showCompleted) {
        /**
         * Create a copy using an immutable set to avoid a
         * {@link java.util.ConcurrentModificationException}
         */
        //获取所有任务id
        Set<Integer> keySet = ImmutableSet.copyOf(operations.keySet());

        if (showCompleted) {
            return new ArrayList<Integer>(keySet);
        }

        List<Integer> keyList = new ArrayList<Integer>();
        for (int key : keySet) {
            AsyncOperation operation = operations.get(key);
            if (operation != null && !operation.getStatus().isComplete()) {
                //将未完成的任务插入到结果集中
                keyList.add(key);
            }
        }
        //返回结果
        return keyList;
    }

    // TODO: 2018/4/4 by zmyer
    public AsyncOperationStatus getOperationStatus(int requestId) {
        if (!operations.containsKey(requestId)) {
            throw new VoldemortException("No operation with id " + requestId + " found");
        }
        //返回任务状态
        return operations.get(requestId).getStatus();
    }

    // Wrapper to avoid throwing an exception over JMX
    // TODO: 2018/4/4 by zmyer
    @JmxOperation
    public String stopAsyncOperation(int requestId) {
        try {
            //停止任务
            stopOperation(requestId);
        } catch (VoldemortException e) {
            return e.getMessage();
        }

        return "Stopping operation " + requestId;
    }

    // TODO: 2018/4/4 by zmyer
    public void stopOperation(int requestId) {
        if (!operations.containsKey(requestId)) {
            throw new VoldemortException("No operation with id " + requestId + " found");
        }

        operations.get(requestId).stop();
    }

    /**
     * Generate a unique request id
     *
     * @return A new, guaranteed unique, request id
     */
    public int getUniqueRequestId() {
        return lastOperationId.getAndIncrement();
    }

    // TODO: 2018/4/4 by zmyer
    @Override
    protected void startInner() {
        logger.info("Starting asyncOperationRunner");
    }

    // TODO: 2018/4/4 by zmyer
    @Override
    protected void stopInner() {
        logger.info("Stopping asyncOperationRunner");
    }

    // TODO: 2018/4/4 by zmyer
    @JmxGetter(name = "totalWaitTime",
            description = "Cumulative number of seconds spent by all tasks in Queue Waiting")
    public long totalWaitTime() {
        long totalWaitTimeMs = 0;
        Set<Integer> keySet = ImmutableSet.copyOf(operations.keySet());
        for (int key : keySet) {
            AsyncOperation operation = operations.get(key);
            if (operation != null && !operation.getStatus().isComplete()) {
                totalWaitTimeMs += operation.getWaitTimeMs();
            }
        }
        return TimeUnit.SECONDS.convert(totalWaitTimeMs, TimeUnit.MILLISECONDS);
    }

    // TODO: 2018/4/4 by zmyer
    @JmxGetter(name = "numWaitingTasks", description = "Total number of tasks waiting in the Queue")
    public long waitingTasks() {
        long waitingTasks = 0;
        Set<Integer> keySet = ImmutableSet.copyOf(operations.keySet());
        for (int key : keySet) {
            AsyncOperation operation = operations.get(key);
            if (operation != null && !operation.getStatus().isComplete()) {
                if (operation.isWaiting()) {
                    waitingTasks++;
                }
            }
        }
        return waitingTasks;
    }

}
