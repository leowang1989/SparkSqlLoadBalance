public class PoolInfo {
    // 资源池名
    private String poolName;

    // 最小核数
    private int minShare;

    // 权重
    private int poolWeight;

    // 调度模式
    private String mode;

    // 正在运行的SQL个数
    private int activeStages;

    // 正在运行的task个数
    private int runningTaskNum;

    public String getPoolName() {
        return poolName;
    }

    public int getMinShare() {
        return minShare;
    }

    public int getPoolWeight() {
        return poolWeight;
    }

    public String getMode() {
        return mode;
    }

    public int getActiveStages() {
        return activeStages;
    }

    public int getRunningTaskNum() {
        return runningTaskNum;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public void setMinShare(int minShare) {
        this.minShare = minShare;
    }

    public void setPoolWeight(int poolWeight) {
        this.poolWeight = poolWeight;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public void setActiveStages(int activeStages) {
        this.activeStages = activeStages;
    }

    public void setRunningTaskNum(int runningTaskNum) {
        this.runningTaskNum = runningTaskNum;
    }
}
