public class MyStatement implements Statement
{
    private String poolName = "default";

    @Override
    public ResultSet executeQuery(String sql) throws SQLException
    {
        if (!execute(sql))
        {
            throw new SQLException("The query did not generate a result set!");
        }
        return resultSets.get(routedClientCacheKey);
    }

    @Override
    public boolean execute(String sql) throws SQLException
    {
        long startTime = System.nanoTime();
        JsqlParseJudgeTypeImpl parser = new JsqlParseJudgeTypeImpl();
        SqlRouteType sqlRouteType = parser.judgeSqlType(sql);
        LOG.info("Current sql route type is {}", sqlRouteType.getSqlType());

        if (SqlRouteType.AUXILIARY.equals(sqlRouteType))
        {
            // 获取资源池名称
            if (sql.contains("scheduler.pool")) {
                String split[] = sql.split("=");
                if (split.length == 2) {
                    this.poolName = split[1];
                }
            }

            return executeOnAllServers(sql);
        }
        else
        {
            thriftServerRoute(sqlRouteType);
            return executeOnRoutedServer(sql);
        }
    }

    private void thriftServerRoute(SqlRouteType sqlRouteType) throws SQLException
    {
        SqlRouteType assignedRouteType = sqlRouteType;

        // 负载均衡，尝试3次
        List<InstanceInfo> typeRoutedThriftServers = new ArrayList<>();
        for (int cnt = 0; cnt < 3; ++cnt) {
            typeRoutedThriftServers = connection.getConnParams().getRouteTypeAndThriftHostIp().get(sqlRouteType.getSqlType());

            if (typeRoutedThriftServers == null || typeRoutedThriftServers.isEmpty())
            {
                // there is no ThriftServer which type is SqlRouteType.OTHER or SqlRouteType.DDL
                // define ANALYSIS as default sql route type
                assignedRouteType = SqlRouteType.ANALYSIS;
                typeRoutedThriftServers = connection.getRouteTypeAndThriftServers().get(assignedRouteType.getSqlType());
            }

            // check again after manual assign
            if (typeRoutedThriftServers == null || typeRoutedThriftServers.isEmpty())
            {
                throw new SQLException("There is no available thriftserver for SQL type " + sqlRouteType);
            }

            typeRoutedThriftServers = loadBalance(typeRoutedThriftServers);
            if (!typeRoutedThriftServers.isEmpty()) {
                break;
            }

            // 等待一会重试
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOG.error("sleep error, ", e);
            }
        }

        if (typeRoutedThriftServers.isEmpty())
        {
            throw new SQLException("There is no resource");
        }

        // define the first analysis thriftserver sorted as DDL
        if (SqlRouteType.DDL.equals(sqlRouteType) && SqlRouteType.ANALYSIS.equals(assignedRouteType))
        {
            Object[] stortedServers = typeRoutedThriftServers.toArray();
            Arrays.sort(stortedServers);

            this.routedClientCacheKey = new ClientCacheKey(SqlRouteType.ANALYSIS.getSqlType(),
                String.valueOf(stortedServers[NUMBER_ZERO]));
        }
        else
        {
            InstanceInfo thriftServerRouted = typeRoutedThriftServers.get(random.nextInt(typeRoutedThriftServers.size()));
            this.routedClientCacheKey = new ClientCacheKey(assignedRouteType.getSqlType(), thriftServerRouted.getAddr());
        }

        LOG.debug("The sql route result: {}", routedClientCacheKey);
    }

    /**
     * 根据当前资源池进行负载均衡
     *
     * @param typeRoutedThriftServers 实例集合
     * @return
     */
    private List<InstanceInfo> loadBalance(List<InstanceInfo> typeRoutedThriftServers)
    {
        List<InstanceInfo> candidates = new ArrayList<>();

        // step1: 根据资源池名筛选实例
        for (InstanceInfo instance : typeRoutedThriftServers) {
            if (instance.containsPool(poolName)) {
                candidates.add(instance);
            }
        }

        if (candidates.size() <= 1) {
            return candidates;
        }

        // step2: 如果是FIFO资源池选取任务数最少的
        if (candidates.get(0).getPool(poolName).getMode().equals("FIFO")) {
            int minStagesNum = -1;
            List<InstanceInfo> tmp = new ArrayList<>();
            for (InstanceInfo instance : candidates) {
                int curStagesNum = instance.getPool(poolName).getActiveStages();
                if (minStagesNum == -1 || curStagesNum < minStagesNum) {
                    tmp.clear();
                    tmp.add(instance);
                    minStagesNum = curStagesNum;
                } else if (curStagesNum == minStagesNum) {
                    tmp.add(instance);
                }
            }

            candidates = tmp;
            if (candidates.size() <= 1) {
                return candidates;
            }
        }

        // step3: 从候选实例列表中选取当前资源池负载最低的实例
        List<InstanceInfo> tmp = new ArrayList<>();
        int maxPoolAvail = 0;
        for (InstanceInfo instance : candidates) {
            PoolInfo pool = instance.getPool(poolName);
            // 资源池可用核数
            int poolAvail = (int) (instance.getMaxCoreNum() * (pool.getPoolWeight() * 1.0f / instance.getTotalWeight()) - pool.getRunningTaskNum());
            // 实例可用核数
            int instAvail = instance.getMaxCoreNum() - instance.getRunningTaskNum();
            // 取两者较小的一个
            int avail = Math.min(poolAvail, instAvail);
            if (avail == 0 || avail < maxPoolAvail) {
                continue;
            }

            if (avail > maxPoolAvail) {
                tmp.clear();
                maxPoolAvail = avail;
            }

            tmp.add(instance);
        }

        // 返回当前资源池可用核数最多的实例
        if (!tmp.isEmpty()) {
            candidates = tmp;
            return candidates;
        }

        // step4: 从候选实例中选取总负载最低的实例
        int avail = 0;
        InstanceInfo candidate = null;
        for (InstanceInfo instance : candidates) {
            int instAvail = instance.getMaxCoreNum() - instance.getRunningTaskNum();
            if (instAvail > avail) {
                avail = instAvail;
                candidate = instance;
            }
        }

        if (candidate != null) {
            candidates.clear();
            candidates.add(candidate);
        } else {
            // 若候选者为空且资源池为FIFO，返回所有实例，排队执行
            if (candidates.get(0).getPool(poolName).getMode().equals("FIFO")) {
                return candidates;
            }
        }

        // 没有候选实例，返回空列表
        candidates.clear();
        return candidates;
    }
}
