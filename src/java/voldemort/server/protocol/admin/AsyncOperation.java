package voldemort.server.protocol.admin;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;

/**
 */
// TODO: 2018/4/4 by zmyer
public abstract class AsyncOperation implements Runnable {

    //异步操作状态信息
    protected final AsyncOperationStatus status;

    // TODO: 2018/4/4 by zmyer
    public AsyncOperation(int id, String description) {
        this.status = new AsyncOperationStatus(id, description);
    }

    // TODO: 2018/4/4 by zmyer
    @JmxGetter(name = "asyncTaskStatus")
    public AsyncOperationStatus getStatus() {
        return status;
    }

    // TODO: 2018/4/4 by zmyer
    public void updateStatus(String msg) {
        status.setStatus(msg);
    }

    // TODO: 2018/4/4 by zmyer
    public void markComplete() {
        status.setComplete(true);

    }

    // TODO: 2018/4/4 by zmyer
    public void run() {
        //更新状态
        updateStatus("Started " + getStatus());
        //获取线程名
        String previousThreadName = Thread.currentThread().getName();
        //设置当前线程名
        Thread.currentThread().setName(previousThreadName + "; AsyncOp ID " + status.getId());
        try {
            //开始执行操作
            operate();
        } catch (Exception e) {
            status.setException(e);
        } finally {
            //执行完毕，需要及时更新线程名
            Thread.currentThread().setName(previousThreadName);
            //标记完成状态
            updateStatus("Finished " + getStatus());
            markComplete();
        }
    }

    // TODO: 2018/4/4 by zmyer
    @Override
    public String toString() {
        return getStatus().toString();
    }

    // TODO: 2018/4/4 by zmyer
    abstract public void operate() throws Exception;

    // TODO: 2018/4/4 by zmyer
    @JmxOperation
    abstract public void stop();

    /**
     * Cumulative wait time is reported as Seconds in the JMX by AsyncService.
     * AsyncOperation can return a positive number indicating the milliseconds
     * it is waiting, to be included for that calculation.
     *
     * @return If the AsyncOperation is waiting, return the wait time in
     *         milliseconds else return 0
     */
    public long getWaitTimeMs() {
        return 0;
    }

    /**
     * Number of wait tasks is reported as Seconds in the JMX by AsyncService.
     * AsyncOperation can return true to indicate that it should be included in
     * that calculation.
     *
     * @return If the AsyncOperation is waiting, return true else false
     */
    public boolean isWaiting() {
        return false;
    }
}
