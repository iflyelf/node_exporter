# 主机部署清单采集器（deployment_inventory_collector）

`deployment_inventory` 用于采集主机上**已安装/已部署的服务与中间件清单**，
并将结果以 Prometheus 指标暴露。采集字段包括服务名、服务路径、运行用户、启动方式、
中间件类型、工作目录（cwd）、可执行文件（exe）、根目录（root）等。

- **默认排除系统服务、内核线程、桌面组件和开发工具进程**
- **默认不采集 Docker/Kubernetes 容器，容器 cgroup 内进程也默认跳过，避免高基数与卡死**
- **全异步刷新，`/metrics` 只读内存快照，不阻塞 Prometheus 抓取**
- **默认每 24 小时刷新一次，首次启动立即在后台加载**
- **不执行任何外部命令，不 readlink 业务路径，只读取 procfs 与本地配置文件**
- **单个数据源失败时保留其上一次成功结果，避免指标突然消失**

---

## 1. 采集来源

| 数据源 | 说明 |
|--------|------|
| `systemd` | 解析 `/etc/systemd/system`、`/run/systemd/system`、`/usr/local/lib/systemd/system`、`/usr/lib/systemd/system`、`/lib/systemd/system` 下的 `.service` 单元 |
| `initd` | 扫描 `/etc/init.d`，通过 `/etc/rc*.d/S*` 判断是否启用 |
| `upstart` | 解析 `/etc/init/*.conf` 与 `.override`，兼容 Ubuntu 14、RHEL 6 等 Upstart 系统 |
| `supervisor` | 解析 `/etc/supervisor/supervisord.conf`、`/etc/supervisor/conf.d/*.conf`、`/etc/supervisord.conf`、`/etc/supervisord.d/*` 中的 program |
| `process` | 并发扫描 `/proc/<pid>`，读取 `cmdline`、`environ(PWD)`、`cgroup`、`status(UID)` 等 procfs 内存文件 |

进程扫描会通过 cgroup 将进程关联到 systemd 单元，并与 systemd/supervisor/initd 条目合并去重。
**Docker / Kubernetes containerd 容器默认不采集**，容器 cgroup 内进程也默认跳过，
以避免 Pod/容器数量过多导致高基数、命令卡死或 node_exporter 资源占用过高。
如需采集容器内进程，可显式设置 `DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES=true`。

---

## 2. 指标说明

### 2.1 服务/中间件清单

```text
node_deployment_inventory_info{
  service_name, service_path, service_user, startup_type, middleware,
  cwd, exe, root, status, enabled, source
} 1
```

| 标签 | 含义 |
|------|------|
| `service_name` | 服务名（systemd 单元名去后缀、进程名或 jar 名） |
| `service_path` | 服务路径（ExecStart、init/upstart 脚本、可执行文件等） |
| `service_user` | 运行用户；优先本地 `/etc/passwd`，否则显示数字 UID |
| `startup_type` | 启动方式：`systemd` / `initd` / `upstart` / `supervisor` / `process` |
| `middleware` | 中间件类型（`nginx`、`mysql`、`redis`、`kafka` 等），未识别为 `none` |
| `cwd` | 进程工作目录，优先取 `environ` 中的 `PWD`；systemd 取 `WorkingDirectory`；否则 `unknown` |
| `exe` | 可执行文件路径，取自 `cmdline[0]`，不读取 `/proc/<pid>/exe` 链接 |
| `root` | systemd 取 `RootDirectory`（默认 `/`）；进程条目固定 `unknown`，不读取 `/proc/<pid>/root` |
| `status` | `running`（systemd cgroup 命中或进程命中）/ `unknown`（无匹配进程，无法在不执行 systemctl 的情况下确认 inactive/failed） |
| `enabled` | 是否开机启动：`true` / `false` / `static` / `unknown` |
| `source` | 数据来源：`systemd` / `initd` / `upstart` / `supervisor` / `process` |

### 2.2 辅助指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `node_deployment_inventory_services_total` | Gauge | 服务/中间件总数 |
| `node_deployment_inventory_ready` | Gauge | 首次加载是否完成（1=完成，0=加载中） |
| `node_deployment_inventory_last_refresh_timestamp_seconds` | Gauge | 最近一次刷新完成时间（Unix 时间戳） |
| `node_deployment_inventory_refresh_duration_seconds` | Gauge | 最近一次刷新耗时 |
| `node_deployment_inventory_refresh_errors_total` | Counter | 累计数据源刷新失败次数 |
| `node_deployment_inventory_source_up{source}` | Gauge | 各数据源最近一次刷新是否成功（失败时在缓存 TTL 内沿用上次结果） |
| `node_deployment_inventory_source_last_success_timestamp_seconds{source}` | Gauge | 各数据源最后一次采集成功时间（0=从未成功） |

---

## 3. 环境变量

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `DEPLOYMENT_INVENTORY_INTERVAL` | `24h` | 清单刷新周期 |
| `DEPLOYMENT_INVENTORY_TIMEOUT` | `90s` | 单次刷新总超时 |
| `DEPLOYMENT_INVENTORY_CONCURRENCY` | `min(NumCPU*2, 16)` | systemd 单元/进程扫描并发度 |
| `DEPLOYMENT_INVENTORY_HOST_ROOT` | 空 | 容器内采集宿主机时，宿主机根目录挂载点，如 `/host` |
| `DEPLOYMENT_INVENTORY_PROC` | `PROC_PREFIX` 或 `/proc` | `/proc` 路径，兼容容器挂载 |
| `DEPLOYMENT_INVENTORY_SYSTEMD` | `true` | 是否采集 systemd 服务 |
| `DEPLOYMENT_INVENTORY_INITD` | `true` | 是否采集 init.d 服务 |
| `DEPLOYMENT_INVENTORY_UPSTART` | `true` | 是否采集 Upstart 服务（Ubuntu 14 / RHEL 6） |
| `DEPLOYMENT_INVENTORY_SUPERVISOR` | `true` | 是否采集 supervisor 配置 |
| `DEPLOYMENT_INVENTORY_PROCESS` | `true` | 是否扫描 `/proc` 进程 |
| `DEPLOYMENT_INVENTORY_INCLUDE_USER_PROCESSES` | `true` | 是否采集用户会话中的进程；设为 `false` 只保留系统级部署 |
| `DEPLOYMENT_INVENTORY_INCLUDE_DISABLED_INITD` | `false` | 是否包含未启用的 init.d 脚本 |
| `DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES` | `false` | 是否采集容器 cgroup 内进程（默认关闭，避免容器/Pod 高基数与卡死） |
| `DEPLOYMENT_INVENTORY_EXCLUDE` | 空 | 额外排除项，逗号分隔，按名称子串匹配 |
| `DEPLOYMENT_INVENTORY_INCLUDE` | 空 | 强制包含项（优先级高于系统服务排除），逗号分隔 |
| `DEPLOYMENT_INVENTORY_CACHE_TTL` | `24h` | 数据源刷新失败时旧缓存最长保留时间，超过后丢弃该数据源旧数据 |

> 建议采集宿主机时挂载 `/proc`、`/etc/systemd`、`/etc/init.d`、`/etc/init`、`/etc/supervisor`、
> 并设置 `DEPLOYMENT_INVENTORY_HOST_ROOT` 与 `DEPLOYMENT_INVENTORY_PROC`。

---

## 3.1 老系统兼容性

| 系统 | 采集方式 | 说明 |
|------|----------|------|
| RHEL / CentOS 6 | SysV init + Upstart | 解析 `/etc/init.d`、`/etc/rc.d/rc*.d/S*`、`/etc/init/*.conf`；无 systemd 时自动跳过 |
| SUSE / SLES 12 | systemd + SysV 兼容层 | 解析 systemd 单元、`/etc/init.d/rc*.d/S*` |
| Ubuntu 14 | Upstart | 解析 `/etc/init/*.conf`，读取 `exec`、`script`、`setuid`、`chdir`、`start on` 与 `.override` |

老系统上请注意：

1. 使用 CI 的 **legacy 二进制**（Go 1.23 编译，支持 kernel 2.6.32–3.10）；
2. SysV init 脚本会解析 `DAEMON`/`PROG`/`USER`/`DAEMON_USER`/`chdir`，并与 `/proc` 进程按 exe 合并，兼容“服务名与进程名不一致”的场景；
3. 无 systemd 时 `status` 主要来自进程合并，`systemd` 数据源为空但不会报错；

---

## 3.2 容器与网络存储安全

### 容器采集

为避免 Docker / Kubernetes containerd 容器数量过多导致高基数、命令卡死或资源占用过高，
采集器**默认不采集容器清单**，并默认跳过容器 cgroup 内的进程
（`DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES=false`）。

如确需将容器内进程纳入部署清单，可显式设置：

```sh
export DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES=true
```

> 该开关只影响容器内进程，不会执行 `docker ps` / `crictl` 等容器运行时命令；
> 大规模 Pod 环境建议保持关闭。

### 无阻塞采集设计

采集器**不执行任何外部命令**（systemctl/supervisorctl/initctl/docker/crictl 均不调用），
也**不读取** `/proc/<pid>/{exe,cwd,root}` 链接，因此不会因业务 NFS/Ceph 路径进入 D 状态：

- `exe` 取自 `cmdline[0]`；
- `cwd` 取自 `environ` 中的 `PWD`；
- 进程条目的 `root` 固定为 `unknown`，systemd 条目取配置中的 `RootDirectory`；
- 运行状态由进程 cgroup 合并得到，不再调用 systemctl/initctl/supervisorctl；
- systemd/init.d/upstart/supervisor 配置目录/文件、以及 `DEPLOYMENT_INVENTORY_PROC` 路径，
  如果位于网络文件系统，通过 `/proc/self/mountinfo` 判断后直接跳过，不 open/stat 这些路径；
- `/etc/passwd` 仅在本地普通文件时读取，否则用户显示为 UID 数字。

> `/proc` 是内存虚拟文件系统，读取 cmdline/cgroup/status/environ 不涉及业务磁盘路径。
> 配置目录在本地磁盘时才会读取；网络挂载配置目录会被跳过。



---

## 4. 异步与性能设计

1. **后台刷新**：`NewDeploymentInventoryCollector` 只启动一个后台协程，首次刷新异步执行；  
   `Collect()` 仅通过 `atomic.Pointer` 读取不可变快照，抓取延迟与主机进程数无关。
2. **每日刷新**：默认 `ticker` 周期 24 小时，刷新完成后一次性替换快照。
3. **并发采集**：systemd/initd/upstart/supervisor/process 数据源并发执行；
   systemd 单元与 `/proc` 进程均有固定大小工作池；进程扫描只读 procfs。
4. **超时控制**：整体刷新有 `context` 超时（默认 90s），并额外设置硬超时（+15s）；
   不执行外部命令，不存在子进程管道等待问题。
5. **卡死跳过**：每个数据源有独立的执行中标记；若上一轮数据源卡住未返回，
   下一轮刷新直接跳过该数据源并沿用缓存，不会持续堆积 goroutine，也不会阻塞 `/metrics`。
6. **防重入**：`sync.Mutex.TryLock()` 保证上一轮刷新未结束时不会重复进入，且不阻塞抓取。
7. **防死锁**：刷新过程中不持有全局锁执行 I/O；各数据源结果先写入本地集合，最后统一合并；
   快照发布使用原子替换，无锁竞争。
8. **缓存过期**：数据源失败时仅在 `DEPLOYMENT_INVENTORY_CACHE_TTL`（默认 24h）内沿用旧结果；
   超过 TTL 后主动丢弃该数据源缓存，保证不会出现“数据永远不更新”。
   每个数据源的最后成功时间可通过 `node_deployment_inventory_source_last_success_timestamp_seconds` 观察。
9. **标签长度限制**：路径、cwd、exe、命令行等标签按字符数截断（256–2048 字符），
   避免超长路径或异常命令行撑大指标体积。
10. **服务去重**：先按服务名（忽略大小写与 `.service/.sh` 等后缀）合并同名服务，
    再按最终标签组合二次去重，保证同一服务只有一条指标且不会触发重复指标错误。
11. **内存控制**：`/proc` 采用流式读取，避免一次性保存全部 PID；挂载信息每次刷新重新解析；
    UID 用户名缓存有上限，防止长期增长。
12. **无界条目采集**：服务清单不设条数上限；通过系统服务过滤、容器进程跳过、同名去重
    控制最终指标规模。

---

## 5. 使用示例

```sh
# node_exporter 启动参数
./node_exporter --collector.deployment_inventory

# 自定义刷新周期与并发
export DEPLOYMENT_INVENTORY_INTERVAL=24h
export DEPLOYMENT_INVENTORY_CONCURRENCY=8
export DEPLOYMENT_INVENTORY_EXCLUDE=java-test,python-demo
```

在容器中运行 node_exporter 采集宿主机：

```sh
docker run --rm --privileged --pid=host \
  -v /proc:/host/proc:ro \
  -v /:/host:ro \
  -e DEPLOYMENT_INVENTORY_PROC=/host/proc \
  -e DEPLOYMENT_INVENTORY_HOST_ROOT=/host \
  -e DEPLOYMENT_INVENTORY_INTERVAL=24h \
  -p 9100:9100 \
  your_image_name \
  --collector.deployment_inventory
```

---

## 6. PromQL 示例

```promql
# 所有业务服务与中间件
node_deployment_inventory_info{source!="process"}

# 中间件清单
node_deployment_inventory_info{middleware!="none"}

# 非 root 用户运行的服务
node_deployment_inventory_info{service_user!="root"}

# 运行中但未开机启动的服务
node_deployment_inventory_info{status="running",enabled="false"}

# 清单是否已完成首次加载
node_deployment_inventory_ready == 0

# 超过 24 小时未成功采集的数据源（数据可能过期）
time() - node_deployment_inventory_source_last_success_timestamp_seconds > 86400

# 部署清单条目数量（观察基数）
node_deployment_inventory_services_total
```

---

## 7. 常见问题

- **服务列表为空？**
  - 确认 `node_deployment_inventory_ready` 是否为 1；首次加载在后台执行，稍后重试。
  - 确认目标服务未被系统服务/桌面组件过滤规则命中，可通过 `DEPLOYMENT_INVENTORY_INCLUDE` 强制包含。
- **看不到 `nohup` 启动的进程？**
  - 默认包含用户会话进程；若设置了 `DEPLOYMENT_INVENTORY_INCLUDE_USER_PROCESSES=false`，请改回 `true`。
- **会采集 Docker / Kubernetes containerd 容器吗？**
  - 默认不采集。容器数量多时容易造成高基数和运行时卡死，采集器默认跳过容器 cgroup 内进程。
    如确需容器内进程，可设置 `DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES=true`。
- **node_exporter 运行在容器里，如何采集宿主机？**
  - 需挂载宿主机 `/proc` 并设置 `DEPLOYMENT_INVENTORY_PROC`、`DEPLOYMENT_INVENTORY_HOST_ROOT`。
- **为什么很多服务 `status="unknown"`？**
  - 采集器不再执行 `systemctl/initctl/supervisorctl`，systemd 服务状态通过 `/proc/<pid>/cgroup` 补全：
    只要单元有任意进程，即使该进程被工具/容器过滤，也会被标记为 `running`。
  - 完全没有进程的单元可能是 stopped/inactive/failed，无法在不执行 systemctl 的情况下区分，
    因此保持 `unknown`，避免误报为 inactive。
  - 若 `DEPLOYMENT_INVENTORY_PROCESS=false`，进程 cgroup 状态不可用，配置类服务会保持 unknown。
- **清单更新不及时？**
  - 默认 24 小时刷新一次，可通过 `DEPLOYMENT_INVENTORY_INTERVAL` 调整；修改后需重启 node_exporter。
- **指标数量过多/抓取变慢？**
  - 通过 `DEPLOYMENT_INVENTORY_EXCLUDE` 排除测试/临时进程。
- **某个数据源一直失败，旧数据会保留多久？**
  - 默认最多 `DEPLOYMENT_INVENTORY_CACHE_TTL=24h`；超过后丢弃该数据源旧缓存。
    默认刷新周期与 TTL 同为 24h，因此一次失败后下一轮就会丢弃旧数据；
    若希望容忍一次失败，可将 `DEPLOYMENT_INVENTORY_CACHE_TTL` 调为 `48h`。
    可通过 `node_deployment_inventory_source_last_success_timestamp_seconds` 判断数据是否过期。
- **采集卡住会不会拖垮 node_exporter？**
  - 不会。`/metrics` 只读内存快照；刷新有整体超时与硬超时，卡住的数据源下一轮会被跳过，
    不会阻塞抓取或无限堆积 goroutine。
