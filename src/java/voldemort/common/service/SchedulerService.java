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

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.utils.Time;

import javax.management.MBeanOperationInfo;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The voldemort scheduler
 *
 *
 */
// TODO: 2018/4/3 by zmyer
@SuppressWarnings("unchecked")
@JmxManaged(description = "A service that runs scheduled jobs.")
public class SchedulerService extends AbstractService {
    //日志对象
    private static final Logger logger = Logger.getLogger(SchedulerService.class);
    //是否中断
    private boolean mayInterrupt;
    //线程名前缀
    private static final String THREAD_NAME_PREFIX = "voldemort-scheduler-service";
    //服务调度次数
    private static final AtomicInteger schedulerServiceCount = new AtomicInteger(0);
    //调度服务名称
    private final String schedulerName = THREAD_NAME_PREFIX + schedulerServiceCount.incrementAndGet();

    // TODO: 2018/4/3 by zmyer
    private class ScheduledRunnable {
        //执行器
        private Runnable runnable;
        //延迟日期
        private Date delayDate;
        //执行间隔时间
        private long intervalMs;

        // TODO: 2018/4/3 by zmyer
        ScheduledRunnable(Runnable runnable, Date delayDate, long intervalMs) {
            this.runnable = runnable;
            this.delayDate = delayDate;
            this.intervalMs = intervalMs;
        }

        // TODO: 2018/4/3 by zmyer
        ScheduledRunnable(Runnable runnable, Date delayDate) {
            this(runnable, delayDate, 0);
        }

        Runnable getRunnable() {
            return this.runnable;
        }

        Date getDelayDate() {
            return this.delayDate;
        }

        long getIntervalMs() {
            return this.intervalMs;
        }
    }

    //调度器
    private final ScheduledThreadPoolExecutor scheduler;
    //计时器
    private final Time time;

    //任务调度结果列表
    private final Map<String, ScheduledFuture> scheduledJobResults;
    //任务队列
    private final Map<String, ScheduledRunnable> allJobs;

    // TODO: 2018/4/3 by zmyer
    public SchedulerService(int schedulerThreads, Time time) {
        this(schedulerThreads, time, true);
    }

    // TODO: 2018/4/3 by zmyer
    public SchedulerService(int schedulerThreads, Time time, boolean mayInterrupt) {
        super(ServiceType.SCHEDULER);
        this.time = time;
        this.scheduler = new SchedulerThreadPool(schedulerThreads, schedulerName);
        this.scheduledJobResults = new ConcurrentHashMap<String, ScheduledFuture>();
        this.allJobs = new ConcurrentHashMap<String, ScheduledRunnable>();
        this.mayInterrupt = mayInterrupt;
    }

    // TODO: 2018/4/3 by zmyer
    @Override
    public void startInner() {
        // TODO note that most code does not do this. so scheduler.isStarted()
        // returns false even after you have submitted some tasks and they are
        // running fine.
    }

    // TODO: 2018/4/3 by zmyer
    @Override
    public void stopInner() {
        this.scheduler.shutdownNow();
        try {
            //关闭调度器
            this.scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.info("Error waiting for termination of scheduler service", e);
        }
    }

    // TODO: 2018/4/3 by zmyer
    @JmxOperation(description = "Disable a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void disable(String id) {
        if (allJobs.containsKey(id) && scheduledJobResults.containsKey(id)) {
            //从结果集中取出指定的正在运行的任务
            ScheduledFuture<?> future = scheduledJobResults.get(id);
            //取消任务执行
            boolean cancelled = future.cancel(false);
            if (cancelled == true) {
                logger.info("Removed '" + id + "' from list of scheduled jobs");
                scheduledJobResults.remove(id);
            }
        }
    }

    // TODO: 2018/4/3 by zmyer
    @JmxOperation(description = "Terminate a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void terminate(String id) {
        if (allJobs.containsKey(id) && scheduledJobResults.containsKey(id)) {
            ScheduledFuture<?> future = scheduledJobResults.get(id);
            boolean cancelled = future.cancel(this.mayInterrupt);
            if (cancelled == true) {
                logger.info("Removed '" + id + "' from list of scheduled jobs");
                scheduledJobResults.remove(id);
            }
        }
    }

    // TODO: 2018/4/3 by zmyer
    @JmxOperation(description = "Enable a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void enable(String id) {
        if (allJobs.containsKey(id) && !scheduledJobResults.containsKey(id)) {
            //从任务队列中获取待执行的任务
            ScheduledRunnable scheduledRunnable = allJobs.get(id);
            logger.info("Adding '" + id + "' to list of scheduled jobs");
            if (scheduledRunnable.getIntervalMs() > 0) {
                //开始调度任务
                schedule(id,
                        scheduledRunnable.getRunnable(),
                        scheduledRunnable.getDelayDate(),
                        scheduledRunnable.getIntervalMs());
            } else {
                //开始执行任务
                schedule(id, scheduledRunnable.getRunnable(), scheduledRunnable.getDelayDate());
            }

        }
    }

    // TODO: 2018/4/3 by zmyer
    @JmxGetter(name = "getScheduledJobs", description = "Returns names of jobs in the scheduler")
    public List<String> getScheduledJobs() {
        return Lists.newArrayList(scheduledJobResults.keySet());
    }

    // TODO: 2018/4/3 by zmyer
    public List<String> getAllJobs() {
        return Lists.newArrayList(allJobs.keySet());
    }

    // TODO: 2018/4/3 by zmyer
    public boolean getJobEnabled(String id) {
        if (allJobs.containsKey(id)) {
            return scheduledJobResults.containsKey(id);
        } else {
            throw new VoldemortException("Job id " + id + " does not exist.");
        }
    }

    // TODO: 2018/4/3 by zmyer
    public void scheduleNow(Runnable runnable) {
        scheduler.execute(runnable);
    }

    // TODO: 2018/4/3 by zmyer
    public void schedule(String id, Runnable runnable, Date timeToRun) {
        //开始调度
        ScheduledFuture<?> future = scheduler.schedule(runnable,
                delayMs(timeToRun),
                TimeUnit.MILLISECONDS);
        if (!allJobs.containsKey(id)) {
            //将运行的任务插入到任务队列中，方便跟踪
            allJobs.put(id, new ScheduledRunnable(runnable, timeToRun));
        }
        //将任务执行结果插入到结果集中
        scheduledJobResults.put(id, future);
    }

    // TODO: 2018/4/3 by zmyer
    public void schedule(String id, Runnable runnable, Date nextRun, long periodMs) {
        schedule(id, runnable, nextRun, periodMs, false);
    }

    // TODO: 2018/4/3 by zmyer
    public void schedule(String id,
            Runnable runnable,
            Date nextRun,
            long periodMs,
            boolean scheduleAtFixedRate) {
        ScheduledFuture<?> future = null;
        if (scheduleAtFixedRate)
        //开始调度任务
        {
            future = scheduler.scheduleAtFixedRate(runnable,
                    delayMs(nextRun),
                    periodMs,
                    TimeUnit.MILLISECONDS);
        } else
        //延时调度任务
        {
            future = scheduler.scheduleWithFixedDelay(runnable,
                    delayMs(nextRun),
                    periodMs,
                    TimeUnit.MILLISECONDS);
        }
        if (!allJobs.containsKey(id)) {
            allJobs.put(id, new ScheduledRunnable(runnable, nextRun, periodMs));
        }
        //将执行结果插入到结果集中
        scheduledJobResults.put(id, future);
    }

    // TODO: 2018/4/3 by zmyer
    private long delayMs(Date runDate) {
        return Math.max(0, runDate.getTime() - time.getMilliseconds());
    }

    /**
     * A scheduled thread pool that fixes some default behaviors
     */
    // TODO: 2018/4/3 by zmyer
    private static class SchedulerThreadPool extends ScheduledThreadPoolExecutor {
        public SchedulerThreadPool(int numThreads, final String schedulerName) {
            super(numThreads, new ThreadFactory() {
                //统计线程数目
                private AtomicInteger threadCount = new AtomicInteger(0);

                /**
                 * This function is overridden in order to activate the daemon mode as well as
                 * to give a human readable name to threads used by the {@link SchedulerService}.
                 *
                 * Previously, this function would set the thread's name to the value of the passed-in
                 * {@link Runnable}'s class name, but this is useless since it always ends up being a
                 * java.util.concurrent.ThreadPoolExecutor$Worker
                 *
                 * Instead, a generic name is now used, and the thread's name can be set more
                 * precisely during {@link voldemort.server.protocol.admin.AsyncOperation#run()}.
                 *
                 * @param r {@link Runnable} to execute
                 * @return a new {@link Thread} appropriate for use within the {@link SchedulerService}.
                 */
                //创建线程
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName(schedulerName + "-t" + threadCount.incrementAndGet());
                    return thread;
                }
            });
        }
    }

    // TODO: 2018/4/3 by zmyer
    @JmxGetter(name = "numActiveTasks", description = "Returns number of tasks executing currently")
    public long getActiveTasksCount() {
        return this.scheduler.getActiveCount();
    }

    // TODO: 2018/4/3 by zmyer
    @JmxGetter(name = "numQueuedTasks",
            description = "Returns number of tasks queued for execution")
    public long getQueuedTasksCount() {
        return this.scheduler.getQueue().size();
    }

}
