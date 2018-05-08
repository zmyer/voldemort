/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.common.service;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.utils.Utils;

/**
 * A helper template for implementing VoldemortService
 * 
 * 
 */
// TODO: 2018/3/23 by zmyer
public abstract class AbstractService implements VoldemortService {

    //日志对象
    private static final Logger logger = Logger.getLogger(VoldemortService.class);

    //启动标记
    private final AtomicBoolean isStarted;

    //服务类型
    private final ServiceType type;

    // TODO: 2018/3/23 by zmyer
    public AbstractService(ServiceType type) {
        this.type = Utils.notNull(type);
        this.isStarted = new AtomicBoolean(false);
    }

    public ServiceType getType() {
        return type;
    }

    // TODO: 2018/3/23 by zmyer
    @JmxGetter(name = "started", description = "Determine if the service has been started.")
    public boolean isStarted() {
        return isStarted.get();
    }

    // TODO: 2018/3/23 by zmyer
    @JmxOperation(description = "Start the service.", impact = MBeanOperationInfo.ACTION)
    public void start() {
        boolean isntStarted = isStarted.compareAndSet(false, true);
        if(!isntStarted)
            throw new IllegalStateException("Server is already started!");

        logger.info("Starting " + getType().getDisplayName());
        //启动服务
        startInner();
    }

    // TODO: 2018/3/23 by zmyer
    @JmxOperation(description = "Stop the service.", impact = MBeanOperationInfo.ACTION)
    public void stop() {
        logger.info("Stopping " + getType().getDisplayName());
        synchronized(this) {
            if(!isStarted()) {
                logger.info("The service is already stopped, ignoring duplicate attempt.");
                return;
            }

            //关闭服务
            stopInner();
            isStarted.set(false);
        }
    }

    protected abstract void startInner();

    protected abstract void stopInner();

}
