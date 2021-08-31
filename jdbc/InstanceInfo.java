public class InstanceInfo {
    // 实例地址
    private String addr;

    // 正在执行的SQL个数
    private int runningSqlNum;

    // 正在运行的Task个数
    private int runningTaskNum;

    // 当前总核数
    private int totalCoreNum;

    // 最大核数
    private int maxCoreNum;

    // 资源池负载
    private List<PoolInfo> pools = new ArrayList<>();

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public int getRunningSqlNum() {
        return runningSqlNum;
    }

    public void setRunningSqlNum(int runningSqlNum) {
        this.runningSqlNum = runningSqlNum;
    }

    public int getRunningTaskNum() {
        return runningTaskNum;
    }

    public void setRunningTaskNum(int runningTaskNum) {
        this.runningTaskNum = runningTaskNum;
    }

    public int getTotalCoreNum() {
        return totalCoreNum;
    }

    public void setTotalCoreNum(int totalCoreNum) {
        this.totalCoreNum = totalCoreNum;
    }

    public int getMaxCoreNum() {
        return maxCoreNum;
    }

    public void setMaxCoreNum(int maxCoreNum) {
        this.maxCoreNum = maxCoreNum;
    }

    public List<PoolInfo> getPools() {
        return pools;
    }

    public void setPools(List<PoolInfo> pools) {
        this.pools = pools;
    }

    public boolean containsPool(String poolName) {
        for (PoolInfo pool : pools) {
            if (pool.getPoolName().equals(poolName)) {
                return true;
            }
        }

        return false;
    }

    public PoolInfo getPool(String poolName) {
        for (PoolInfo pool : pools) {
            if (pool.getPoolName().equals(poolName)) {
                return pool;
            }
        }

        return null;
    }

    public int getTotalWeight() {
        int weight = 0;
        for (PoolInfo pool : pools) {
            weight += pool.getPoolWeight();
        }

        return weight;
    }

    public static class InstanceInfoDesrializer implements JsonDeserializer<InstanceInfo> {

        @Override
        public InstanceInfo deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject instanceObj = json.getAsJsonObject();
            InstanceInfo instance = new InstanceInfo();
            instance.setRunningSqlNum(instanceObj.get("runningSqlNum").getAsInt());
            instance.setRunningTaskNum(instanceObj.get("runningTaskNum").getAsInt());
            instance.setTotalCoreNum(instanceObj.get("totalCoreNum").getAsInt());
            instance.setMaxCoreNum(instanceObj.get("maxCoreNum").getAsInt());
            JsonArray pools = instanceObj.get("poolLoadInfo").getAsJsonArray();
            List<PoolInfo> poolInfoList = new ArrayList<>();
            pools.forEach(pool -> {
                JsonObject poolObj = pool.getAsJsonObject();
                PoolInfo poolInfo = new PoolInfo();
                poolInfo.setPoolName(poolObj.get("poolName").getAsString());
                poolInfo.setMinShare(poolObj.get("minShare").getAsInt());
                poolInfo.setPoolWeight(poolObj.get("poolWeight").getAsInt());
                poolInfo.setMode(poolObj.get("mode").getAsString());
                poolInfo.setActiveStages(poolObj.get("activeStages").getAsInt());
                poolInfo.setRunningTaskNum(poolObj.get("runningTaskNum").getAsInt());
                poolInfoList.add(poolInfo);
            });

            instance.setPools(poolInfoList);
            return instance;
        }
    }
}
