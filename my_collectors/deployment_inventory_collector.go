// Package my_collectors 提供 Prometheus 指标收集器
//
// 本文件实现「主机部署清单采集器」（deployment_inventory）。
//
// 设计目标：
//  1. 采集主机上已安装/已部署的服务与中间件清单；
//  2. 采集字段包含：服务名、服务路径、运行用户、启动方式、中间件类型、
//     工作目录(cwd)、可执行文件(exe)、根目录(root) 等；
//  3. 默认排除操作系统自带服务、内核线程与容器进程，只保留业务部署与中间件；
//  4. 全异步刷新：Collect() 只读取内存快照，绝不阻塞 /metrics；
//  5. 默认每 24 小时刷新一次，首次启动立即在后台刷新；
//  6. 所有外部命令带超时，进程扫描使用固定大小工作池，避免高并发/死锁；
//  7. 任一数据源刷新失败时保留该数据源上一次成功结果，避免指标突然消失。
package my_collectors

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// deploymentDefaultInterval 默认刷新周期（一天一次）
	deploymentDefaultInterval = 24 * time.Hour
	// deploymentDefaultTimeout 单次刷新总超时
	deploymentDefaultTimeout = 90 * time.Second
	// deploymentHardExtraTimeout 超时后额外等待时间，避免个别数据源卡死刷新协程
	deploymentHardExtraTimeout = 15 * time.Second
	// deploymentMaxFileSize 单个配置文件最大读取字节数（1 MiB）
	deploymentMaxFileSize = 1 << 20
	// deploymentDefaultCacheTTL 数据源刷新失败时旧缓存默认保留时间
	deploymentDefaultCacheTTL = 24 * time.Hour
	// deploymentCmdlineScanLimit 命令行匹配时只扫描首尾 8 KiB，避免超长命令行拖慢采集
	deploymentCmdlineScanLimit = 8 * 1024
)

// deploymentInventoryService 单条服务/中间件部署信息
type deploymentInventoryService struct {
	Name        string // 服务名（systemd 单元名去掉 .service、进程名等）
	Path        string // 服务路径（ExecStart、init/upstart 脚本、可执行文件等）
	User        string // 运行用户
	StartupType string // 启动方式：systemd/initd/upstart/supervisor/process
	Middleware  string // 中间件类型，未识别为 none
	Cwd         string // 进程工作目录
	Exe         string // 可执行文件路径
	Root        string // 进程根目录
	Status      string // 运行状态：running/inactive/unknown
	Enabled     string // 是否开机启动：true/false/static/unknown
	Source      string // 数据来源：systemd/initd/upstart/supervisor/process

	dedupKey  string // 内部去重键，不对外暴露
	instances int    // 进程实例数，内部合并使用
}

// deploymentInventorySnapshot 只读快照，发布后不再修改
type deploymentInventorySnapshot struct {
	services          []deploymentInventoryService
	lastRefresh       time.Time
	duration          time.Duration
	sourceUp          map[string]float64
	sourceLastSuccess map[string]time.Time
}

var (
	// deploymentInventorySnapshotPtr 原子指针，Collect 无锁读取
	deploymentInventorySnapshotPtr atomic.Pointer[deploymentInventorySnapshot]

	// deploymentInventoryRefreshMu 保证同一时刻只有一个刷新任务在跑
	deploymentInventoryRefreshMu sync.Mutex

	// deploymentInventoryStartOnce 保证后台刷新协程只启动一次
	deploymentInventoryStartOnce sync.Once

	// deploymentInventoryRefreshErrors 累计刷新错误次数（按数据源失败次数计）
	deploymentInventoryRefreshErrors atomic.Uint64

	// deploymentInventorySourceRunning 记录各数据源是否仍有一轮采集未返回
	deploymentInventorySourceRunning sync.Map

	// deploymentInventoryHostUsers 缓存宿主机 /etc/passwd 解析结果
	deploymentInventoryHostUsersOnce sync.Once
	deploymentInventoryHostUsersMap  map[int]string

	// deploymentInventoryUserCache 缓存 UID -> 用户名，避免重复 NSS 查询
	deploymentInventoryUserCache      sync.Map
	deploymentInventoryUserCacheCount atomic.Int64

	// deploymentNetworkFSCache 缓存 mount namespace -> 是否包含网络文件系统
	deploymentNetworkFSCache      sync.Map
	deploymentNetworkFSCacheCount atomic.Int64

	// 额外包含/排除项解析一次，避免每个进程/单元重复 split+分配
	deploymentExtraExcludesOnce sync.Once
	deploymentExtraExcludesList []string
	deploymentExtraIncludesOnce sync.Once
	deploymentExtraIncludesList []string
)

// ==========================================================================
// 环境变量解析
// ==========================================================================

func deploymentEnvDuration(name string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

func deploymentEnvInt(name string, def int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func deploymentEnvBool(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

// deploymentInventoryConcurrency 返回进程/单元文件扫描并发度
func deploymentInventoryConcurrency() int {
	def := runtime.NumCPU() * 2
	if def < 4 {
		def = 4
	}
	if def > 16 {
		def = 16
	}
	n := deploymentEnvInt("DEPLOYMENT_INVENTORY_CONCURRENCY", def)
	if n > 64 {
		n = 64
	}
	return n
}

// deploymentHostRoot 返回宿主机根目录（容器部署时用于读取宿主机配置）
func deploymentHostRoot() string {
	root := strings.TrimSpace(os.Getenv("DEPLOYMENT_INVENTORY_HOST_ROOT"))
	if root == "" || root == "/" {
		return ""
	}
	return filepath.Clean(root)
}

// deploymentHostPath 将宿主机绝对路径映射到容器内可见路径
func deploymentHostPath(p string) string {
	root := deploymentHostRoot()
	if root == "" {
		return p
	}
	return filepath.Join(root, strings.TrimPrefix(p, "/"))
}

// deploymentProcRoot 返回 /proc 路径前缀，兼容容器挂载 /host/proc
func deploymentProcRoot() string {
	if v := strings.TrimSpace(os.Getenv("DEPLOYMENT_INVENTORY_PROC")); v != "" {
		return filepath.Clean(v)
	}
	if v := strings.TrimSpace(os.Getenv("PROC_PREFIX")); v != "" {
		return filepath.Clean(v)
	}
	return "/proc"
}

// ==========================================================================
// 系统服务/内核线程/交互式进程过滤
// ==========================================================================

// deploymentSystemNames 系统服务或系统守护进程黑名单
// 说明：这里只排除操作系统基础服务，nginx/mysql/redis 等业务中间件不在其中。
var deploymentSystemNames = map[string]struct{}{
	"systemd": {}, "init": {}, "systemd-journald": {}, "systemd-logind": {},
	"systemd-networkd": {}, "systemd-resolved": {}, "systemd-timesyncd": {},
	"systemd-udevd": {}, "systemd-user-sessions": {}, "systemd-coredump": {},
	"dbus": {}, "dbus-daemon": {}, "dbus-broker": {}, "polkit": {}, "polkitd": {},
	"accounts-daemon": {}, "udisks2": {}, "upower": {}, "avahi-daemon": {},
	"modemmanager": {}, "networkmanager": {}, "wpa-supplicant": {},
	"bluetooth": {}, "cups": {}, "cups-browsed": {},
	"ssh": {}, "sshd": {}, "login": {}, "agetty": {}, "getty": {},
	"cron": {}, "crond": {}, "atd": {}, "anacron": {},
	"rsyslog": {}, "rsyslogd": {}, "syslog-ng": {}, "systemd-journald-audit": {},
	"auditd": {}, "kdump": {}, "kexec": {}, "firewalld": {}, "iptables": {},
	"ip6tables": {}, "nftables": {}, "ufw": {}, "fail2ban": {},
	"chronyd": {}, "chrony": {}, "ntpd": {}, "ntp": {}, "irqbalance": {},
	"smartd": {}, "lvm2-lvmpolld": {}, "multipathd": {}, "iscsid": {},
	"iscsi": {}, "nfs-server": {}, "nfs-client": {}, "rpcbind": {},
	"rpc-statd": {}, "rpc-gssd": {}, "rpc-svcgssd": {}, "rpcbind.service": {},
	"sssd": {}, "winbind": {}, "nscd": {}, "oddjobd": {},
	"packagekit": {}, "packagekit-offline-update": {}, "snapd": {},
	"unattended-upgrades": {}, "apt-daily": {}, "apt-daily-upgrade": {},
	"apt-news": {}, "dpkg-db-backup": {}, "man-db": {}, "logrotate": {},
	"e2scrub-all": {}, "e2scrub-reap": {}, "fstrim": {},
	"gdm": {}, "lightdm": {}, "sddm": {}, "display-manager": {},
	"xorg": {}, "xwayland": {}, "pulseaudio": {}, "pipewire": {},
	"containerd-shim": {}, "docker-proxy": {},
	"vgauth": {}, "vmtoolsd": {}, "open-vm-tools": {}, "qemu-guest-agent": {},
	"waagent": {}, "cloud-init": {}, "e2scrub_all": {}, "e2scrub_reap": {},
	"locate": {}, "plocate": {}, "mlocate": {}, "coredump-reporter": {},
	"wpa_supplicant": {}, "mpris-proxy": {}, "gcr-ssh-agent": {},
	"README": {}, "rc": {}, "rcS": {}, "gdomap": {}, "ipsec": {},
	"smartmontools": {}, "blk-availability": {}, "rbdmap": {}, "ldconfig": {},
	"live-config": {}, "replace-lightdm": {}, "grub-common": {},
	"networkmanager-wait-online": {}, "getty-static": {}, "kmod-static-nodes": {},
	"user": {}, "user-runtime-dir": {}, "serial-getty": {},
	"acpid": {}, "paxctld": {}, "procps": {}, "seatd": {}, "sudo": {},
	"tlp": {}, "rsync": {}, "network": {}, "networking": {},
	"mountkernfs": {}, "mountdevsubfs": {}, "checkroot": {}, "checkfs": {},
	"umountroot": {}, "umountfs": {}, "sendsigs": {}, "bootlogs": {},
	"hwclock": {}, "hostname": {}, "killprocs": {}, "single": {},
	"halt": {}, "reboot": {},
	// RHEL / CentOS 6
	"abrtd": {}, "cpuspeed": {}, "netfs": {}, "nfslock": {},
	"rpc.statd": {}, "rpc.idmapd": {}, "rpc.svcgssd": {}, "rpc.gssd": {},
	"mdmonitor": {}, "sysstat": {}, "saslauthd": {}, "certmonger": {},
	"matahari": {}, "piranha": {}, "ricci": {}, "luci": {},
	"libvirtd": {}, "virtlockd": {}, "virtlogd": {}, "xend": {}, "xendomains": {},
	"haldaemon": {}, "messagebus": {}, "readahead": {}, "restorecond": {},
	"mcstrans": {}, "setroubleshoot": {}, "yum-updatesd": {}, "yum-cron": {},
	"netconsole": {}, "postfix": {}, "sendmail": {}, "exim": {},
	"portreserve": {}, "iscsiuio": {}, "fcoe": {}, "lldpad": {}, "dcbd": {},
	"nslcd": {}, "pcscd": {}, "hplip": {}, "xdm": {}, "kdm": {}, "lxdm": {},
	"slim": {}, "mcelog": {}, "rasdaemon": {}, "psacct": {}, "acct": {},
	"autofs": {}, "ypbind": {}, "ypserv": {}, "yppasswdd": {}, "ypxfrd": {},
	// SUSE 12
	"splash": {}, "splash_early": {}, "earlysyslog": {}, "klogd": {}, "syslog": {},
	"after-local": {}, "purge-kernels": {}, "snapper": {}, "btrfsmaintenance": {},
	"wicked": {}, "wickedd": {}, "wickedd-nanny": {},
	"susefirewall2": {}, "susefirewall2_init": {}, "susefirewall2_setup": {},
	"openvswitch": {}, "ovs-vswitchd": {}, "ovsdb-server": {},
	"start-ttys": {}, "control-alt-delete": {}, "init-system-dbus": {}, "prefdm": {},
	"xinetd": {}, "inetd": {}, "rsh": {}, "rlogin": {}, "rexec": {},
	"opensm": {}, "ibacm": {}, "cgmanager": {}, "cgproxy": {},
	// Ubuntu 14 Upstart
	"mountall": {}, "rc-sysinit": {}, "failsafe-x": {}, "ureadahead": {},
	"container-detect": {}, "setvtrgb": {}, "pcmcia": {}, "pcmciautils": {},
	"usbmuxd": {}, "gpu-manager": {},
	"bootlogd": {}, "mountnfs": {}, "umountnfs": {}, "mountoverflowtmp": {},
	"rmnologin": {}, "urandom": {}, "mtab": {}, "binfmt-support": {},
	"module-init-tools": {}, "ondemand": {}, "screen-cleanup": {}, "startpar": {},
	"bridge-utils": {}, "vlan": {}, "ifupdown": {}, "resolvconf": {},
	"friendly-recovery": {}, "open-iscsi": {}, "passwd": {},
}

// deploymentSystemExecBasenames 系统工具类可执行文件，非自定义部署不作为服务展示
var deploymentSystemExecBasenames = map[string]struct{}{
	"mount": {}, "umount": {}, "true": {}, "false": {},
	"ldconfig": {}, "kmod": {}, "modprobe": {}, "insmod": {}, "rmmod": {}, "depmod": {},
	"alsactl": {}, "plymouth": {}, "plymouthd": {}, "grub-editenv": {},
	"nm-online": {}, "rbdmap": {}, "systemctl": {}, "live-config": {},
	"swapon": {}, "swapoff": {}, "fsck": {}, "e2fsck": {}, "mkswap": {},
	"hwclock": {}, "setterm": {}, "loadkeys": {}, "console-setup": {},
	"keyboard-setup": {}, "udevadm": {}, "sysctl": {}, "update-rc.d": {},
	"chkconfig": {}, "service": {}, "init": {}, "shutdown": {}, "reboot": {},
	"poweroff": {}, "telinit": {}, "runlevel": {}, "hostname": {},
	"hostnamectl": {}, "localectl": {}, "timedatectl": {}, "loginctl": {},
	"resolvectl": {}, "networkctl": {}, "busctl": {},
	"systemd-tmpfiles": {}, "systemd-sysusers": {}, "systemd-sysctl": {},
	"systemd-random-seed": {}, "systemd-binfmt": {}, "systemd-modules-load": {},
	"systemd-fsck": {}, "systemd-quotacheck": {}, "systemd-remount-fs": {},
	"systemd-update-utmp": {}, "systemd-machine-id-setup": {}, "systemd-firstboot": {},
	"systemd-hwdb": {}, "systemd-backlight": {}, "systemd-cryptsetup": {},
	"systemd-cryptenroll": {}, "systemd-boot": {}, "systemd-pstore": {},
	"systemd-repart": {}, "systemd-sleep": {}, "systemd-hibernate": {},
	"systemd-suspend": {}, "systemd-udev-settle": {}, "systemd-udev-trigger": {},
	"systemd-journal-flush": {}, "systemd-journal-catalog-update": {},
	"systemd-tmpfiles-clean": {}, "systemd-tmpfiles-setup": {},
	"systemd-update-utmp-runlevel": {}, "systemd-user-sessions": {},
}

// deploymentSystemPrefixes 系统服务/内核线程/桌面会话名称前缀
var deploymentSystemPrefixes = []string{
	"systemd-", "user@", "getty@", "serial-getty@", "console-getty",
	"kworker", "ksoftirqd", "migration", "rcu", "watchdog", "cpuhp",
	"idle_inject", "irq/", "jbd2", "kdmflush", "kmpath", "scsi_",
	"xfs-", "xfs_", "ext4-", "kauditd", "oom_reaper", "kdevtmpfs", "netns",
	"khungtaskd", "writeback", "kintegrityd", "kblockd", "ata_",
	"edac", "devfreq", "zswap", "zram", "kthrotld", "ipv6_addrconf",
	"kstrp", "psimon", "tls-strp", "kcompactd", "khugepaged", "ksmd",
	"kswapd", "numa", "dbus-", "avahi-", "gnome-", "org.deepin", "org.gnome",
	"org.freedesktop", "dde-", "deepin-", "lastore", "linglong", "uos-",
	"trayplugin", "dconf", "gvfs", "xdg-", "ibus-", "fcitx", "at-spi",
	"kde", "plasma", "xfce", "mate-", "cinnamon", "kwin", "gjs",
	"evolution", "tracker", "goa-", "mission-center", "switcheroo",
	"bolt", "colord", "geoclue", "rtkit", "power-profiles-daemon",
	"thermald", "bluetoothd", "cupsd", "e2scrub", "coredump-",
	"apt-", "dpkg", "packagekit", "unattended-upgr",
	"sshd", "ssh-", "vmware", "vmblock", "vmhgfs", "vmmemctl",
	"mpris", "obex", "wireplumber", "pipewire-", "gcr-ssh",
	"plymouth", "lvm2", "alsa", "tpm2", "grub", "kmod", "networkmanager",
	"getty", "mnt-", "media-", "mdadm", "dmraid", "fcoe", "fwupd", "apport",
	"whoopsie", "kerneloops", "pollinate", "apparmor", "selinux",
	"cryptdisks", "paxctl", "procps", "pulseaudio", "seatd", "tlp",
	"x11", "acpi", "console-setup", "keyboard-setup", "replace-lightdm",
	// RHEL / CentOS 6
	"abrt", "iscsi", "nfs", "rpc.", "rpc-", "libvirt", "virt", "xen",
	"readahead", "setroubleshoot", "yum-", "matahari", "piranha", "ipa-",
	"yp", "mcelog", "pcsc", "rdma", "ipoib", "sblim",
	// SUSE 12
	"splash", "earlysyslog", "boot.", "after-", "snapper", "btrfs",
	"wicked", "suse", "openvswitch", "ovs-",
	// Ubuntu 14 Upstart
	"mountall", "mounted-", "network-", "tty", "console", "upstart-",
	"udev", "failsafe", "rc-", "pcmcia", "start-", "init-system", "lxc",
}

// deploymentInteractiveNames 交互式/短生命周期命令黑名单，避免把用户登录态进程当成服务
var deploymentInteractiveNames = map[string]struct{}{
	"bash": {}, "sh": {}, "dash": {}, "zsh": {}, "fish": {}, "ksh": {},
	"csh": {}, "tcsh": {}, "vim": {}, "vi": {}, "nvim": {}, "nano": {},
	"emacs": {}, "less": {}, "more": {}, "tail": {}, "head": {}, "cat": {},
	"grep": {}, "egrep": {}, "fgrep": {}, "sed": {}, "awk": {}, "find": {},
	"locate": {}, "ps": {}, "top": {}, "htop": {}, "atop": {}, "iotop": {},
	"lsof": {}, "ss": {}, "netstat": {}, "ping": {}, "curl": {}, "wget": {},
	"git": {}, "scp": {}, "sftp": {}, "rsync": {}, "ssh": {}, "sudo": {},
	"su": {}, "tmux": {}, "screen": {}, "journalctl": {}, "dmesg": {},
	"apt": {}, "apt-get": {}, "dpkg": {}, "yum": {}, "dnf": {}, "rpm": {},
	"systemctl": {}, "service": {}, "chkconfig": {}, "update-rc.d": {},
	"python -c": {}, "python3 -c": {}, "perl -e": {}, "ruby -e": {},
	"sleep": {}, "wait": {}, "timeout": {}, "watch": {}, "nohup": {},
	"env": {}, "xargs": {}, "tee": {}, "yes": {}, "seq": {}, "test": {},
	"[": {}, "true": {}, "false": {}, "date": {}, "uname": {}, "whoami": {},
	"id": {}, "pwd": {}, "echo": {}, "printf": {}, "mkdir": {}, "rm": {},
	"cp": {}, "mv": {}, "ln": {}, "chmod": {}, "chown": {}, "tar": {},
	"gzip": {}, "gunzip": {}, "zip": {}, "unzip": {}, "dd": {}, "df": {},
	"du": {}, "free": {}, "uptime": {}, "w": {}, "who": {}, "last": {},
	"lastb": {}, "lscpu": {}, "lsblk": {}, "lspci": {}, "lsusb": {},
}

// deploymentExtraExcludes 解析 DEPLOYMENT_INVENTORY_EXCLUDE（逗号分隔，子串匹配）
func deploymentParseEnvList(name string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func deploymentExtraExcludes() []string {
	deploymentExtraExcludesOnce.Do(func() {
		deploymentExtraExcludesList = deploymentParseEnvList("DEPLOYMENT_INVENTORY_EXCLUDE")
	})
	return deploymentExtraExcludesList
}

// deploymentExtraIncludes 解析 DEPLOYMENT_INVENTORY_INCLUDE（逗号分隔，子串匹配，优先级高于排除规则）
func deploymentExtraIncludes() []string {
	deploymentExtraIncludesOnce.Do(func() {
		deploymentExtraIncludesList = deploymentParseEnvList("DEPLOYMENT_INVENTORY_INCLUDE")
	})
	return deploymentExtraIncludesList
}

// deploymentTruncateLabel 按字符数截断标签值，防止超长路径/命令行造成指标膨胀
func deploymentTruncateLabel(value string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes])
}

// deploymentServiceIdentity 服务去重身份：忽略大小写与常见单元后缀，
// 但保留 @instance，避免把不同实例误合并。
func deploymentServiceIdentity(name string) string {
	identity := strings.ToLower(strings.TrimSpace(name))
	for _, suffix := range []string{".service", ".timer", ".socket", ".sh"} {
		identity = strings.TrimSuffix(identity, suffix)
	}
	return identity
}

func deploymentNormalizeServiceName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.TrimSuffix(name, ".service")
	name = strings.TrimSuffix(name, ".timer")
	name = strings.TrimSuffix(name, ".socket")
	name = strings.TrimSuffix(name, ".sh")
	if idx := strings.Index(name, "@"); idx >= 0 {
		name = name[:idx]
	}
	return name
}

// deploymentIsSystemService 判断是否系统服务（默认排除）
func deploymentIsSystemService(name string) bool {
	base := deploymentNormalizeServiceName(name)
	if base == "" {
		return true
	}
	for _, p := range deploymentExtraIncludes() {
		if strings.Contains(base, p) {
			return false
		}
	}
	if _, ok := deploymentSystemNames[base]; ok {
		return true
	}
	for _, p := range deploymentSystemPrefixes {
		if strings.HasPrefix(base, p) {
			return true
		}
	}
	for _, p := range deploymentExtraExcludes() {
		if strings.Contains(base, p) {
			return true
		}
	}
	return false
}

// deploymentIsInteractiveProcess 判断是否交互式/短生命周期进程
func deploymentIsInteractiveProcess(name string) bool {
	base := deploymentNormalizeServiceName(name)
	if _, ok := deploymentInteractiveNames[base]; ok {
		return true
	}
	for _, p := range deploymentExtraExcludes() {
		if strings.Contains(base, p) {
			return true
		}
	}
	return false
}

var deploymentToolingPrefixes = []string{
	"code-server", "code-", "vscode", "opencode", "copilot", "codex",
	"ptyserver", "ptyhost", "extensionhost", "tsserver", "gopls",
	"rust-analyzer", "clangd", "pyright", "jedi-language", "gcr-ssh",
	"mpris", "ssh-agent", "wireplumber", "obex", "dconf", "gvfs",
}

var deploymentToolingPathMarkers = []string{
	"/.vscode-server/", "/.vscode/", "/.cursor-server/", "/.cursor/",
	"/.opencode/", "/.codex/", "/.cache/", "/.npm/", "/.nvm/",
	"/.config/nvm/", "/.local/lib/", "/node_modules/", "/.local/share/",
}

// deploymentIsToolingProcess 判断是否开发工具/编辑器/桌面辅助进程（默认不纳入部署清单）
func deploymentIsToolingProcess(name, exe, cmdline string) bool {
	lowerName := strings.ToLower(name)
	for _, p := range deploymentToolingPrefixes {
		if strings.HasPrefix(lowerName, p) {
			return true
		}
	}
	for _, text := range []string{exe, deploymentCommandTextForScan(cmdline)} {
		lower := strings.ToLower(text)
		for _, marker := range deploymentToolingPathMarkers {
			if strings.Contains(lower, marker) {
				return true
			}
		}
	}
	return false
}

// deploymentIsSystemPath 判断可执行文件是否属于系统服务目录
func deploymentIsSystemPath(path string) bool {
	if path == "" {
		return false
	}
	for _, p := range []string{"/usr/lib/systemd/", "/lib/systemd/", "/usr/libexec/systemd/"} {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// deploymentIsSystemExec 判断可执行文件是否系统工具（不作为业务服务展示）
func deploymentIsSystemExec(path string) bool {
	if path == "" || path == "unknown" {
		return false
	}
	_, ok := deploymentSystemExecBasenames[strings.ToLower(filepath.Base(path))]
	return ok
}

// ==========================================================================
// 中间件识别
// ==========================================================================

// deploymentMiddlewareAliases 中间件别名映射（key 为小写 token，value 为展示名）
var deploymentMiddlewareAliases = map[string]string{
	"nginx": "nginx", "openresty": "nginx", "tengine": "nginx",
	"httpd": "apache", "apache2": "apache", "apache": "apache",
	"mysqld": "mysql", "mysql": "mysql", "mariadbd": "mariadb", "mariadb": "mariadb",
	"postgres": "postgresql", "postmaster": "postgresql", "postgresql": "postgresql",
	"redis": "redis", "redis-server": "redis",
	"mongod": "mongodb", "mongodb": "mongodb",
	"elasticsearch": "elasticsearch", "opensearch": "opensearch",
	"rabbitmq": "rabbitmq", "rabbitmq-server": "rabbitmq",
	"kafka": "kafka", "zookeeper": "zookeeper", "rocketmq": "rocketmq",
	"activemq": "activemq", "pulsar": "pulsar", "emqx": "emqx", "mosquitto": "mosquitto",
	"tomcat": "tomcat", "catalina": "tomcat", "jetty": "jetty",
	"wildfly": "wildfly", "jboss": "jboss",
	"docker": "docker", "dockerd": "docker", "containerd": "containerd", "podman": "podman",
	"kubelet": "kubernetes", "kube-apiserver": "kubernetes", "kube-proxy": "kubernetes",
	"etcd": "etcd", "consul": "consul", "nacos": "nacos",
	"memcached": "memcached", "clickhouse": "clickhouse", "clickhouse-server": "clickhouse",
	"influxd": "influxdb", "influxdb": "influxdb",
	"prometheus": "prometheus", "grafana": "grafana", "grafana-server": "grafana",
	"filebeat": "filebeat", "logstash": "logstash", "kibana": "kibana",
	"haproxy": "haproxy", "keepalived": "keepalived", "traefik": "traefik", "caddy": "caddy",
	"minio": "minio", "jenkins": "jenkins", "nexus": "nexus", "gitlab": "gitlab",
	"harbor": "harbor", "sonarqube": "sonarqube", "zabbix": "zabbix",
	"gunicorn": "gunicorn", "uwsgi": "uwsgi",
	"oracle": "oracle", "tnslsnr": "oracle", "sqlservr": "sqlserver",
	"cassandra": "cassandra", "hbase": "hbase", "hive": "hive",
	"hadoop": "hadoop", "spark": "spark", "sentinel": "sentinel", "seata": "seata",
}

// deploymentCommandTextForScan 截取命令行的首尾片段用于匹配，避免超长 cmdline 造成大字符串分配。
func deploymentCommandTextForScan(text string) string {
	if len(text) <= deploymentCmdlineScanLimit {
		return text
	}
	half := deploymentCmdlineScanLimit / 2
	return text[:half] + " " + text[len(text)-half:]
}

// deploymentDetectMiddleware 从名称、路径、命令行、镜像名中识别中间件
func deploymentDetectMiddleware(name, path, cmdline, image string) string {
	cmdline = deploymentCommandTextForScan(cmdline)
	texts := []string{name, path, cmdline, image}
	for _, text := range texts {
		lower := strings.ToLower(text)
		if lower == "" {
			continue
		}
		// 先整体精确匹配（如 mysqld、redis-server）
		normalized := deploymentNormalizeServiceName(filepath.Base(lower))
		if m, ok := deploymentMiddlewareAliases[normalized]; ok {
			return m
		}
		// 再按 token 匹配（如 nginx:1.25、kafka_2.13-3.6.0.jar）
		tokens := strings.FieldsFunc(lower, func(r rune) bool {
			return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
		})
		for _, token := range tokens {
			if m, ok := deploymentMiddlewareAliases[token]; ok {
				return m
			}
		}
	}
	return "none"
}

// ==========================================================================
// 通用文件读取与用户解析
// ==========================================================================

func deploymentReadFileLimited(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit))
}

func deploymentCleanProcPath(p string) string {
	p = strings.TrimSuffix(strings.TrimSpace(p), " (deleted)")
	if p == "" {
		return "unknown"
	}
	return p
}

// deploymentLoadHostUsers 解析宿主机 /etc/passwd，兼容容器内 UID 与宿主机不一致的场景
func deploymentLoadHostUsers() map[int]string {
	deploymentInventoryHostUsersOnce.Do(func() {
		deploymentInventoryHostUsersMap = make(map[int]string)
		data, err := deploymentReadFileLimited(deploymentHostPath("/etc/passwd"), deploymentMaxFileSize)
		if err != nil {
			return
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Split(line, ":")
			if len(fields) < 3 {
				continue
			}
			uid, err := strconv.Atoi(fields[2])
			if err != nil {
				continue
			}
			deploymentInventoryHostUsersMap[uid] = fields[0]
		}
	})
	return deploymentInventoryHostUsersMap
}

func deploymentUserName(uid int) string {
	if uid < 0 {
		return "unknown"
	}
	if cached, ok := deploymentInventoryUserCache.Load(uid); ok {
		return cached.(string)
	}
	name := ""
	if n, ok := deploymentLoadHostUsers()[uid]; ok && n != "" {
		name = n
	}
	if name == "" {
		if u, err := user.LookupId(strconv.Itoa(uid)); err == nil && u.Username != "" {
			name = u.Username
		}
	}
	if name == "" {
		name = strconv.Itoa(uid)
	}
	// 限制缓存规模，避免异常 UID 空间导致内存持续增长
	if deploymentInventoryUserCacheCount.Load() < 8192 {
		if _, loaded := deploymentInventoryUserCache.LoadOrStore(uid, name); !loaded {
			deploymentInventoryUserCacheCount.Add(1)
		}
	}
	return name
}

// ==========================================================================
// 后台刷新调度
// ==========================================================================

func deploymentInventorySafeRefresh(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[deployment_inventory] refresh panic recovered: %v", r)
		}
	}()
	deploymentInventoryRefreshOnce(ctx)
}

// startDeploymentInventoryWorker 启动后台刷新协程（幂等）
func startDeploymentInventoryWorker() {
	deploymentInventoryStartOnce.Do(func() {
		go func() {
			// 首次刷新异步执行，不阻塞 node_exporter 启动与 /metrics 暴露
			deploymentInventorySafeRefresh(context.Background())

			interval := deploymentEnvDuration("DEPLOYMENT_INVENTORY_INTERVAL", deploymentDefaultInterval)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for range ticker.C {
				deploymentInventorySafeRefresh(context.Background())
			}
		}()
	})
}

type deploymentInventorySourceResult struct {
	name     string
	services []deploymentInventoryService
	err      error
}

type deploymentInventorySource struct {
	name    string
	collect func(context.Context) ([]deploymentInventoryService, error)
}

// deploymentInventoryCommand 创建带超时和 WaitDelay 的外部命令。
// WaitDelay 确保子进程被 context 取消后，即使有孙进程继承 stdout/stderr 管道，
// Output() 也不会无限等待管道关闭。
func deploymentInventoryCommand(ctx context.Context, timeout time.Duration, name string, args ...string) (*exec.Cmd, context.CancelFunc) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	cmd := exec.CommandContext(cctx, name, args...)
	cmd.WaitDelay = 2 * time.Second
	return cmd, cancel
}

type deploymentLimitedBuffer struct {
	buf   bytes.Buffer
	limit int
}

func (b *deploymentLimitedBuffer) Write(p []byte) (int, error) {
	remaining := b.limit - b.buf.Len()
	if remaining > 0 {
		if len(p) <= remaining {
			_, _ = b.buf.Write(p)
		} else {
			_, _ = b.buf.Write(p[:remaining])
		}
	}
	// 始终返回 len(p)，避免命令因短写异常退出；超出部分丢弃以限制内存
	return len(p), nil
}

// deploymentInventoryCommandOutput 执行外部命令并限制 stdout/stderr 内存占用。
func deploymentInventoryCommandOutput(ctx context.Context, timeout time.Duration, limit int64, name string, args ...string) ([]byte, error) {
	cmd, cancel := deploymentInventoryCommand(ctx, timeout, name, args...)
	defer cancel()
	stdout := &deploymentLimitedBuffer{limit: int(limit)}
	stderr := &deploymentLimitedBuffer{limit: 64 << 10}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", name, err, strings.TrimSpace(stderr.buf.String()))
	}
	return stdout.buf.Bytes(), nil
}

// deploymentInventoryAcquireSource 获取数据源执行权；若上一轮同名数据源仍未返回，
// 本轮直接跳过，避免卡死的数据源在每次刷新时不断堆积 goroutine。
func deploymentInventoryAcquireSource(name string) (*atomic.Bool, bool) {
	value, _ := deploymentInventorySourceRunning.LoadOrStore(name, &atomic.Bool{})
	flag := value.(*atomic.Bool)
	if !flag.CompareAndSwap(false, true) {
		return flag, false
	}
	return flag, true
}

// deploymentInventorySources 返回本次启用的数据源
func deploymentInventorySources() []deploymentInventorySource {
	sources := make([]deploymentInventorySource, 0, 6)
	if deploymentEnvBool("DEPLOYMENT_INVENTORY_SYSTEMD", true) {
		sources = append(sources, deploymentInventorySource{name: "systemd", collect: deploymentCollectSystemd})
	}
	if deploymentEnvBool("DEPLOYMENT_INVENTORY_INITD", true) {
		sources = append(sources, deploymentInventorySource{name: "initd", collect: deploymentCollectInitd})
	}
	if deploymentEnvBool("DEPLOYMENT_INVENTORY_UPSTART", true) {
		sources = append(sources, deploymentInventorySource{name: "upstart", collect: deploymentCollectUpstart})
	}
	if deploymentEnvBool("DEPLOYMENT_INVENTORY_SUPERVISOR", true) {
		sources = append(sources, deploymentInventorySource{name: "supervisor", collect: deploymentCollectSupervisor})
	}
	if deploymentEnvBool("DEPLOYMENT_INVENTORY_PROCESS", true) {
		sources = append(sources, deploymentInventorySource{name: "process", collect: deploymentCollectProcesses})
	}
	return sources
}

// deploymentInventoryRefreshOnce 执行一次完整刷新并原子发布快照
func deploymentInventoryRefreshOnce(parent context.Context) {
	// TryLock 防止上一轮刷新尚未结束时重复进入；不阻塞调用方
	if !deploymentInventoryRefreshMu.TryLock() {
		return
	}
	defer deploymentInventoryRefreshMu.Unlock()

	start := time.Now()
	timeout := deploymentEnvDuration("DEPLOYMENT_INVENTORY_TIMEOUT", deploymentDefaultTimeout)
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	sources := deploymentInventorySources()
	prev := deploymentInventorySnapshotPtr.Load()

	results := make(map[string]deploymentInventorySourceResult, len(sources))
	resultCh := make(chan deploymentInventorySourceResult, len(sources))

	launched := 0
	for _, src := range sources {
		flag, ok := deploymentInventoryAcquireSource(src.name)
		if !ok {
			// 上一轮该数据源仍未返回（例如卡在 NFS/命令管道），本轮直接跳过，
			// 继续使用上一次成功快照，避免阻塞刷新和堆积 goroutine。
			results[src.name] = deploymentInventorySourceResult{
				name: src.name,
				err:  fmt.Errorf("previous collection still running, skipped this round"),
			}
			log.Printf("[deployment_inventory] source %s is still running from previous refresh, skip this round", src.name)
			continue
		}
		launched++
		go func(s deploymentInventorySource, running *atomic.Bool) {
			defer running.Store(false)
			res := deploymentInventorySourceResult{name: s.name}
			// 无论是否 panic 都必须回传结果，避免刷新流程等待不到该数据源
			defer func() {
				if r := recover(); r != nil {
					res.services, res.err = nil, fmt.Errorf("panic: %v", r)
				}
				resultCh <- res
			}()
			res.services, res.err = s.collect(ctx)
		}(src, flag)
	}

	hardTimer := time.NewTimer(timeout + deploymentHardExtraTimeout)
	defer hardTimer.Stop()

	// 每个成功启动的数据源只会发送一次结果，因此不需要额外的 wg.Wait/close
	// goroutine（数据源永久卡住时那个 goroutine 也会泄漏）。读满 launched 条即可。
	for i := 0; i < launched; i++ {
		select {
		case res := <-resultCh:
			results[res.name] = res
		case <-hardTimer.C:
			// 极端情况下（第三方命令不响应）不再等待，使用已有结果
			log.Printf("[deployment_inventory] refresh hard timeout after %s, use partial results", timeout+deploymentHardExtraTimeout)
			i = launched // 退出循环
		}
	}

	// 失败数据源在缓存 TTL 内保留上一次成功结果，超过 TTL 后丢弃，避免数据永久不更新
	cacheTTL := deploymentEnvDuration("DEPLOYMENT_INVENTORY_CACHE_TTL", deploymentDefaultCacheTTL)
	now := time.Now()
	prevBySource := make(map[string][]deploymentInventoryService)
	prevLastSuccess := make(map[string]time.Time)
	if prev != nil {
		for _, svc := range prev.services {
			prevBySource[svc.Source] = append(prevBySource[svc.Source], svc)
		}
		for source, last := range prev.sourceLastSuccess {
			prevLastSuccess[source] = last
		}
	}

	sourceUp := make(map[string]float64, len(sources))
	sourceLastSuccess := make(map[string]time.Time, len(sources))
	var services []deploymentInventoryService
	var errCount uint64
	var errMsgs []string

	for _, src := range sources {
		res, ok := results[src.name]
		if !ok || res.err != nil {
			sourceUp[src.name] = 0
			errCount++
			if !ok {
				errMsgs = append(errMsgs, fmt.Sprintf("%s: no result (timeout or still running)", src.name))
			} else if res.err != nil {
				errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", src.name, res.err))
			}

			lastSuccess, hasSuccess := prevLastSuccess[src.name]
			reuseCache := hasSuccess && now.Sub(lastSuccess) <= cacheTTL
			if hasSuccess {
				// 即使缓存过期，也保留最后成功时间，便于通过指标发现长期失败的数据源
				sourceLastSuccess[src.name] = lastSuccess
			}
			if reuseCache {
				services = append(services, prevBySource[src.name]...)
			} else if hasSuccess {
				log.Printf("[deployment_inventory] source %s cache expired after %s without success, drop cached data",
					src.name, time.Since(lastSuccess).Round(time.Second))
			}
			continue
		}
		sourceUp[src.name] = 1
		sourceLastSuccess[src.name] = now
		services = append(services, res.services...)
	}

	services = deploymentMergeServices(services)

	snapshot := &deploymentInventorySnapshot{
		services:          services,
		lastRefresh:       time.Now(),
		duration:          time.Since(start),
		sourceUp:          sourceUp,
		sourceLastSuccess: sourceLastSuccess,
	}
	deploymentInventorySnapshotPtr.Store(snapshot)

	if errCount > 0 {
		deploymentInventoryRefreshErrors.Add(errCount)
		log.Printf("[deployment_inventory] refresh completed with %d error(s): %s", errCount, strings.Join(errMsgs, "; "))
	}
	log.Printf("[deployment_inventory] refresh completed: services=%d duration=%s",
		len(snapshot.services), snapshot.duration.Round(time.Millisecond))
}

// deploymentIsGenericRuntime 判断是否为通用运行时（多个业务进程可能共用同一 exe）
func deploymentIsGenericRuntime(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	base = strings.TrimSuffix(base, ".exe")
	switch base {
	case "java", "python", "python2", "python3", "node", "nodejs", "ruby", "perl",
		"php", "php-fpm", "sh", "bash", "dash", "env", "mono", "dotnet",
		"gunicorn", "uwsgi", "supervisord":
		return true
	}
	return false
}

// deploymentMergeServices 合并不同数据源的重复服务，并确保服务名唯一
func deploymentMergeServices(input []deploymentInventoryService) []deploymentInventoryService {
	if len(input) == 0 {
		return nil
	}

	merged := make(map[string]*deploymentInventoryService, len(input))
	nameIndex := make(map[string][]string) // 服务名 -> 配置类服务去重键
	exeIndex := make(map[string][]string)  // 可执行文件 -> 配置类服务去重键

	mergeInto := func(dst, src *deploymentInventoryService) {
		if src.Path != "" && dst.Path == "" {
			dst.Path = src.Path
		}
		if src.Exe != "" && src.Exe != "unknown" {
			dst.Exe = src.Exe
		} else if dst.Exe == "" {
			dst.Exe = "unknown"
		}
		if src.Cwd != "" && src.Cwd != "unknown" {
			dst.Cwd = src.Cwd
		}
		if src.Root != "" && src.Root != "unknown" {
			dst.Root = src.Root
		}
		if src.User != "" && src.User != "unknown" {
			dst.User = src.User
		}
		if src.Status == "running" {
			dst.Status = "running"
		} else if dst.Status == "" || dst.Status == "unknown" {
			if src.Status != "" {
				dst.Status = src.Status
			}
		}
		if dst.Middleware == "none" && src.Middleware != "none" {
			dst.Middleware = src.Middleware
		}
		if src.Enabled == "true" {
			dst.Enabled = "true"
		} else if dst.Enabled == "" || dst.Enabled == "unknown" {
			if src.Enabled != "" {
				dst.Enabled = src.Enabled
			}
		}
		dst.instances += src.instances
	}

	// 第一轮：配置类数据源（systemd/initd/upstart/supervisor）优先
	for i := range input {
		svc := input[i]
		if svc.Source == "process" {
			continue
		}
		key := svc.dedupKey
		if key == "" {
			key = svc.Source + ":" + deploymentNormalizeServiceName(svc.Name)
		}
		if existing, ok := merged[key]; ok {
			mergeInto(existing, &svc)
			continue
		}
		copySvc := svc
		copySvc.dedupKey = key
		merged[key] = &copySvc
		nameKey := deploymentNormalizeServiceName(svc.Name)
		nameIndex[nameKey] = append(nameIndex[nameKey], key)
		if (svc.Source == "initd" || svc.Source == "supervisor" || svc.Source == "upstart") &&
			svc.Exe != "" && svc.Exe != "unknown" && !deploymentIsGenericRuntime(svc.Exe) {
			exeKey := strings.TrimSuffix(svc.Exe, " (deleted)")
			exeIndex[exeKey] = append(exeIndex[exeKey], key)
		}
	}

	// 第二轮：进程扫描结果，优先合并到同名的 systemd/initd/supervisor 条目
	for i := range input {
		svc := input[i]
		if svc.Source != "process" {
			continue
		}

		key := svc.dedupKey
		if key == "" {
			key = "process:" + deploymentNormalizeServiceName(svc.Name) + "|" + svc.Exe + "|" + svc.Cwd + "|" + svc.User
		}

		if existing, ok := merged[key]; ok {
			mergeInto(existing, &svc)
			continue
		}

		// 非 systemd 单元进程：优先按可执行文件路径合并到 initd / supervisor / upstart 配置
		// （老系统常见服务名与进程名不一致，如 initd mysql -> 进程 mysqld）
		if strings.HasPrefix(key, "process:") && svc.Exe != "" && svc.Exe != "unknown" {
			exeKey := strings.TrimSuffix(svc.Exe, " (deleted)")
			if keys := exeIndex[exeKey]; len(keys) == 1 {
				target := merged[keys[0]]
				if target != nil && (target.Source == "initd" || target.Source == "supervisor" || target.Source == "upstart") {
					mergeInto(target, &svc)
					continue
				}
			}
		}

		// 再按名称合并
		if strings.HasPrefix(key, "process:") {
			nameKey := deploymentNormalizeServiceName(svc.Name)
			if keys := nameIndex[nameKey]; len(keys) == 1 {
				target := merged[keys[0]]
				if target != nil && (target.Source == "initd" || target.Source == "supervisor" || target.Source == "upstart") {
					mergeInto(target, &svc)
					continue
				}
			}
		}

		copySvc := svc
		copySvc.dedupKey = key
		merged[key] = &copySvc
	}

	out := make([]deploymentInventoryService, 0, len(merged))
	for _, svc := range merged {
		// 统一空值，避免出现空标签
		if svc.Path == "" {
			svc.Path = "unknown"
		}
		if svc.Exe == "" {
			svc.Exe = "unknown"
		}
		if svc.Cwd == "" {
			svc.Cwd = "unknown"
		}
		if svc.Root == "" {
			svc.Root = "unknown"
		}
		if svc.User == "" {
			svc.User = "unknown"
		}
		if svc.Status == "" {
			svc.Status = "unknown"
		}
		if svc.Enabled == "" {
			svc.Enabled = "unknown"
		}
		if svc.Middleware == "" {
			svc.Middleware = "none"
		}
		// 限制标签长度，防止超长路径/命令行导致指标体积和高基数问题
		svc.Name = deploymentTruncateLabel(svc.Name, 256)
		svc.Path = deploymentTruncateLabel(svc.Path, 1024)
		svc.User = deploymentTruncateLabel(svc.User, 128)
		svc.StartupType = deploymentTruncateLabel(svc.StartupType, 32)
		svc.Middleware = deploymentTruncateLabel(svc.Middleware, 64)
		svc.Cwd = deploymentTruncateLabel(svc.Cwd, 1024)
		svc.Exe = deploymentTruncateLabel(svc.Exe, 1024)
		svc.Root = deploymentTruncateLabel(svc.Root, 512)
		svc.Status = deploymentTruncateLabel(svc.Status, 32)
		svc.Enabled = deploymentTruncateLabel(svc.Enabled, 32)
		svc.Source = deploymentTruncateLabel(svc.Source, 32)
		out = append(out, *svc)
	}

	// 先按数据源优先级排序：配置类（systemd/initd/upstart/supervisor）优先于纯进程。
	// 这样按服务名去重时，保留配置中的启动方式/用户/路径，再用进程信息补全 cwd/exe。
	sourcePriority := func(source string) int {
		switch source {
		case "systemd":
			return 0
		case "initd":
			return 1
		case "upstart":
			return 2
		case "supervisor":
			return 3
		case "process":
			return 4
		default:
			return 5
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		pi, pj := sourcePriority(out[i].Source), sourcePriority(out[j].Source)
		if pi != pj {
			return pi < pj
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Exe != out[j].Exe {
			return out[i].Exe < out[j].Exe
		}
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		if out[i].Cwd != out[j].Cwd {
			return out[i].Cwd < out[j].Cwd
		}
		return out[i].Path < out[j].Path
	})

	// 按服务身份去重：同名服务只保留一条，后续条目合并字段。
	// 服务身份忽略大小写与 .service/.sh 等后缀，但保留 @instance。
	identitySeen := make(map[string]int, len(out))
	canonical := out[:0]
	for i := range out {
		svc := out[i]
		identity := deploymentServiceIdentity(svc.Name)
		if identity == "" {
			identity = strings.ToLower(svc.Name)
		}
		if idx, ok := identitySeen[identity]; ok {
			mergeInto(&canonical[idx], &svc)
			continue
		}
		identitySeen[identity] = len(canonical)
		canonical = append(canonical, svc)
	}
	out = canonical

	// 按最终暴露的标签组合二次去重：不同 dedupKey 的进程可能产生完全相同的标签，
	// 若不合并 Prometheus 会因重复指标导致整个 /metrics 采集失败。
	labelSeen := make(map[string]int, len(out))
	deduped := make([]deploymentInventoryService, 0, len(out))
	for _, svc := range out {
		labelKey := strings.Join([]string{
			svc.Name, svc.Path, svc.User, svc.StartupType, svc.Middleware,
			svc.Cwd, svc.Exe, svc.Root, svc.Status, svc.Enabled, svc.Source,
		}, "\x00")
		if idx, ok := labelSeen[labelKey]; ok {
			deduped[idx].instances += svc.instances
			continue
		}
		labelSeen[labelKey] = len(deduped)
		deduped = append(deduped, svc)
	}
	out = deduped

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].StartupType != out[j].StartupType {
			return out[i].StartupType < out[j].StartupType
		}
		if out[i].Exe != out[j].Exe {
			return out[i].Exe < out[j].Exe
		}
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		return out[i].Cwd < out[j].Cwd
	})

	return out
}

// ==========================================================================
// 数据源：systemd
// ==========================================================================

type deploymentUnitDir struct {
	path   string
	custom bool // /etc/systemd/system 视为管理员自定义部署
}

func deploymentSystemdUnitDirs() []deploymentUnitDir {
	candidates := []deploymentUnitDir{
		{path: "/etc/systemd/system", custom: true},
		{path: "/run/systemd/system", custom: false},
		{path: "/usr/local/lib/systemd/system", custom: false},
		{path: "/usr/lib/systemd/system", custom: false},
		{path: "/lib/systemd/system", custom: false},
	}
	seen := make(map[string]struct{})
	var seenInfo []os.FileInfo
	out := make([]deploymentUnitDir, 0, len(candidates))
	for _, c := range candidates {
		full := deploymentHostPath(c.path)
		info, err := os.Stat(full)
		if err != nil || !info.IsDir() {
			continue
		}
		// 通过 inode 去重，避免 /lib -> /usr/lib 等软链导致重复读取；
		// 容器场景下不能对 /host/lib 做 EvalSymlinks（绝对软链会跳出 host root）。
		duplicate := false
		for _, existing := range seenInfo {
			if os.SameFile(info, existing) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		seenInfo = append(seenInfo, info)
		// 记录真实路径仅用于非容器场景的去重标记
		if deploymentHostRoot() == "" {
			if real, err := filepath.EvalSymlinks(full); err == nil {
				if _, ok := seen[real]; ok {
					continue
				}
				seen[real] = struct{}{}
			}
		}
		out = append(out, deploymentUnitDir{path: full, custom: c.custom})
	}
	return out
}

// deploymentSystemdEnabledUnits 通过 *.wants / *.requires 软链判断 unit 是否开机启用
func deploymentSystemdEnabledUnits(dirs []deploymentUnitDir) map[string]bool {
	enabled := make(map[string]bool)
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasSuffix(name, ".wants") && !strings.HasSuffix(name, ".requires") {
				continue
			}
			subEntries, err := os.ReadDir(filepath.Join(dir.path, name))
			if err != nil {
				continue
			}
			for _, sub := range subEntries {
				enabled[sub.Name()] = true
			}
		}
	}
	return enabled
}

// deploymentSystemdRunningUnits 获取当前 running 的 systemd service（失败则返回空）
func deploymentSystemdRunningUnits(ctx context.Context) (map[string]bool, bool) {
	running := make(map[string]bool)
	if deploymentHostRoot() != "" {
		return running, false
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return running, false
	}
	out, err := deploymentInventoryCommandOutput(ctx, 5*time.Second, 4<<20, "systemctl", "list-units", "--type=service", "--state=running", "--no-pager", "--no-legend", "--plain")
	if err != nil {
		return running, false
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "●")))
		if len(fields) == 0 {
			continue
		}
		if strings.HasSuffix(fields[0], ".service") {
			running[fields[0]] = true
		}
	}
	return running, true
}

type deploymentUnitConfig struct {
	exec            string
	user            string
	workingDir      string
	rootDir         string
	hasInstall      bool
	onCalendar      []string
	onBootSec       string
	onUnitActiveSec string
	timerUnit       string
}

// deploymentParseUnitFile 解析 systemd .service / .timer 文件
func deploymentParseUnitFile(path string) (*deploymentUnitConfig, error) {
	data, err := deploymentReadFileLimited(path, deploymentMaxFileSize)
	if err != nil {
		return nil, err
	}
	cfg := &deploymentUnitConfig{
		user:       "root",
		workingDir: "/",
		rootDir:    "/",
	}
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:idx]))
		value := strings.TrimSpace(line[idx+1:])

		switch section {
		case "unit":
			// [Unit] 段暂不需要额外字段
		case "install":
			cfg.hasInstall = true
		case "service":
			switch key {
			case "execstart":
				if cfg.exec == "" {
					cfg.exec = deploymentParseExecStart(value)
				}
			case "user":
				if value != "" {
					cfg.user = value
				}
			case "workingdirectory":
				if value != "" {
					cfg.workingDir = strings.TrimPrefix(value, "-")
				}
			case "rootdirectory":
				if value != "" {
					cfg.rootDir = strings.TrimPrefix(value, "-")
				}
			}
		case "timer":
			switch key {
			case "oncalendar":
				cfg.onCalendar = append(cfg.onCalendar, value)
			case "onbootsec":
				cfg.onBootSec = value
			case "onunitactivesec":
				cfg.onUnitActiveSec = value
			case "unit":
				cfg.timerUnit = value
			}
		}
	}
	return cfg, scanner.Err()
}

// deploymentParseExecStart 提取 ExecStart 中的可执行文件路径
func deploymentParseExecStart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimLeft(value, "-@+!:")
	fields := strings.Fields(value)
	for i, field := range fields {
		field = strings.Trim(field, `"'`)
		field = strings.TrimLeft(field, "-@+!:")
		if field == "" {
			continue
		}
		// 处理 /usr/bin/env FOO=bar /opt/app 形式
		if field == "env" || strings.HasSuffix(field, "/env") {
			for _, next := range fields[i+1:] {
				next = strings.Trim(next, `"'`)
				if strings.HasPrefix(next, "/") {
					return next
				}
				if strings.Contains(next, "=") {
					continue
				}
				return next
			}
			continue
		}
		// 跳过环境变量赋值
		if strings.Contains(field, "=") && !strings.HasPrefix(field, "/") {
			continue
		}
		return field
	}
	return ""
}

// deploymentCollectSystemd 采集 systemd 服务
func deploymentCollectSystemd(ctx context.Context) ([]deploymentInventoryService, error) {
	dirs := deploymentSystemdUnitDirs()
	if len(dirs) == 0 {
		return nil, nil
	}

	enabled := deploymentSystemdEnabledUnits(dirs)
	running, haveRunning := deploymentSystemdRunningUnits(ctx)

	type unitFile struct {
		name   string
		path   string
		custom bool
	}
	unitMap := make(map[string]unitFile)
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir.path)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".service") || strings.Contains(name, "@.") {
				continue
			}
			// 高优先级目录先占位
			if _, ok := unitMap[name]; ok {
				continue
			}
			unitMap[name] = unitFile{name: name, path: filepath.Join(dir.path, name), custom: dir.custom}
		}
	}

	concurrency := deploymentInventoryConcurrency()
	type unitResult struct {
		svc *deploymentInventoryService
		err error
	}
	jobs := make(chan unitFile)
	results := make(chan unitResult, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[deployment_inventory] systemd worker panic recovered: %v", r)
				}
			}()
			for unit := range jobs {
				if ctx.Err() != nil {
					return
				}
				svc, err := deploymentBuildSystemdService(unit.name, unit.path, unit.custom, enabled, running, haveRunning)
				select {
				case results <- unitResult{svc: svc, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[deployment_inventory] systemd sender panic recovered: %v", r)
			}
		}()
		for _, unit := range unitMap {
			select {
			case jobs <- unit:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var services []deploymentInventoryService
	for res := range results {
		if res.err == nil && res.svc != nil {
			services = append(services, *res.svc)
		}
	}
	if err := ctx.Err(); err != nil {
		return services, err
	}
	return services, nil
}

func deploymentBuildSystemdService(unitName, unitPath string, custom bool, enabled, running map[string]bool, haveRunning bool) (*deploymentInventoryService, error) {
	name := strings.TrimSuffix(unitName, ".service")
	if deploymentIsSystemService(name) {
		return nil, nil
	}
	cfg, err := deploymentParseUnitFile(unitPath)
	if err != nil {
		return nil, err
	}

	middleware := deploymentDetectMiddleware(name, cfg.exec, "", "")
	isEnabled := enabled[unitName]
	isRunning := running[unitName]

	// 非自定义单元中，系统工具类命令（mount/plymouth/ldconfig 等）不视为业务部署
	if !custom && middleware == "none" && deploymentIsSystemExec(cfg.exec) {
		return nil, nil
	}

	// 包管理安装的非中间件服务，仅保留 enabled/running 的，避免系统服务刷屏
	if !custom && !isEnabled && !isRunning && middleware == "none" {
		return nil, nil
	}

	enabledValue := "false"
	if isEnabled {
		enabledValue = "true"
	} else if !cfg.hasInstall {
		enabledValue = "static"
	}

	status := "unknown"
	if haveRunning {
		if isRunning {
			status = "running"
		} else {
			status = "inactive"
		}
	}

	path := cfg.exec
	if path == "" {
		path = unitPath
	}
	return &deploymentInventoryService{
		Name:        name,
		Path:        path,
		User:        cfg.user,
		StartupType: "systemd",
		Middleware:  middleware,
		Cwd:         cfg.workingDir,
		Exe:         cfg.exec,
		Root:        cfg.rootDir,
		Status:      status,
		Enabled:     enabledValue,
		Source:      "systemd",
		dedupKey:    "systemd:" + unitName,
		instances:   0,
	}, nil
}

// ==========================================================================
// 数据源：SysV init.d
// ==========================================================================

func deploymentCollectInitd(ctx context.Context) ([]deploymentInventoryService, error) {
	initDir := deploymentHostPath("/etc/init.d")
	entries, err := os.ReadDir(initDir)
	if err != nil {
		return nil, nil // 目录不存在视为无数据，不报错
	}

	enabled := make(map[string]bool)
	rcPatterns := []string{
		"/etc/rc*.d/S*",
		"/etc/rc.d/rc*.d/S*",
		"/etc/init.d/rc*.d/S*", // SUSE
	}
	for _, pattern := range rcPatterns {
		matches, _ := filepath.Glob(deploymentHostPath(pattern))
		for _, match := range matches {
			base := filepath.Base(match)
			name := strings.TrimLeft(base, "S0123456789")
			if name != "" {
				enabled[name] = true
			}
		}
	}

	var services []deploymentInventoryService
	for _, entry := range entries {
		if ctx.Err() != nil {
			break
		}
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".dpkg-dist") ||
			strings.HasSuffix(name, ".dpkg-old") || strings.HasSuffix(name, ".rpmnew") {
			continue
		}
		if deploymentIsSystemService(name) {
			continue
		}
		path := filepath.Join(initDir, name)
		info, err := entry.Info()
		if err != nil || info.Mode().IsDir() {
			continue
		}
		middleware := deploymentDetectMiddleware(name, path, "", "")
		// 未启用的 init.d 脚本（系统安装但未部署）默认不纳入清单，
		// 可通过 DEPLOYMENT_INVENTORY_INCLUDE_DISABLED_INITD=true 打开。
		if middleware == "none" && !enabled[name] &&
			!deploymentEnvBool("DEPLOYMENT_INVENTORY_INCLUDE_DISABLED_INITD", false) {
			continue
		}
		exe, userName, cwd := deploymentParseInitdScript(path)
		if exe == "" {
			exe = "unknown"
		}
		if userName == "" {
			userName = "unknown"
		}
		if cwd == "" {
			cwd = "unknown"
		}
		services = append(services, deploymentInventoryService{
			Name:        name,
			Path:        path,
			User:        userName,
			StartupType: "initd",
			Middleware:  deploymentDetectMiddleware(name, exe, "", ""),
			Cwd:         cwd,
			Exe:         exe,
			Root:        "/",
			Status:      "unknown",
			Enabled:     strconv.FormatBool(enabled[name]),
			Source:      "initd",
			dedupKey:    "initd:" + name,
			instances:   0,
		})
	}
	if err := ctx.Err(); err != nil {
		return services, err
	}
	return services, nil
}

// deploymentParseInitdScript 从 SysV init 脚本中提取可执行文件、运行用户、工作目录
// 老系统（RHEL/CentOS 6、SUSE 12、Ubuntu 14）很多服务名与进程名不同，
// 解析出 DAEMON/PROG 后可与 /proc 进程按 exe 精确合并。
func deploymentParseInitdScript(path string) (exe, userName, cwd string) {
	data, err := deploymentReadFileLimited(path, 512*1024)
	if err != nil {
		return "", "", ""
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "exec ") {
			if candidate := deploymentParseExecStart(line[len("exec "):]); strings.HasPrefix(candidate, "/") {
				exe = candidate
			}
			continue
		}
		if strings.HasPrefix(lower, "cd ") {
			candidate := strings.Trim(strings.TrimSpace(line[len("cd "):]), `"'`)
			if strings.HasPrefix(candidate, "/") && !strings.ContainsAny(candidate, "$`") {
				cwd = candidate
				continue
			}
		}
		if strings.Contains(lower, "start-stop-daemon") && strings.Contains(lower, "--exec") {
			fields := strings.Fields(line)
			for i, field := range fields {
				if field == "--exec" && i+1 < len(fields) {
					candidate := strings.Trim(fields[i+1], `"'`)
					if strings.HasPrefix(candidate, "/") {
						exe = candidate
					}
				}
			}
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.ToUpper(strings.TrimSpace(line[:idx]))
		value := strings.TrimSpace(line[idx+1:])
		switch key {
		case "DAEMON", "PROG", "EXEC", "BINARY", "DAEMON_BIN", "SERVICE_BIN", "APP_BIN",
			"HTTPD", "MYSQLD", "MYSQL", "NGINX", "REDIS", "POSTGRES", "POSTMASTER",
			"TOMCAT", "JAVA", "NODE", "PYTHON", "GUNICORN", "UWSGI", "SERVER",
			"BIN", "START_BIN", "CMD", "COMMAND":
			if candidate := deploymentExtractAbsolutePath(value); candidate != "" {
				exe = candidate
			}
		case "USER", "DAEMON_USER", "RUN_AS", "RUNAS", "TOMCAT_USER", "MYSQL_USER", "HTTPD_USER":
			candidate := strings.Trim(value, `"'`)
			if candidate != "" && !strings.ContainsAny(candidate, "$%{}`") {
				userName = candidate
			}
		}
	}
	return exe, userName, cwd
}

// deploymentExtractAbsolutePath 从 shell 变量赋值中提取绝对路径
func deploymentExtractAbsolutePath(value string) string {
	for _, token := range strings.Fields(value) {
		token = strings.Trim(token, `"'(){};`)
		idx := strings.Index(token, "/")
		if idx < 0 {
			continue
		}
		candidate := strings.TrimRight(token[idx:], `}")';`)
		if !strings.HasPrefix(candidate, "/") || strings.ContainsAny(candidate, "$`") {
			continue
		}
		lower := strings.ToLower(candidate)
		skip := false
		for _, ext := range []string{
			".pid", ".lock", ".log", ".conf", ".cfg", ".xml",
			".properties", ".json", ".yaml", ".yml", ".socket",
		} {
			if strings.HasSuffix(lower, ext) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		return candidate
	}
	return ""
}

// ==========================================================================
// 数据源：Upstart（Ubuntu 14 / RHEL 6 等老系统）
// ==========================================================================

type deploymentUpstartJob struct {
	name     string
	exec     string
	user     string
	cwd      string
	hasStart bool
	manual   bool
}

// deploymentParseUpstartJob 解析 /etc/init/*.conf
func deploymentParseUpstartJob(path string) (*deploymentUpstartJob, error) {
	data, err := deploymentReadFileLimited(path, deploymentMaxFileSize)
	if err != nil {
		return nil, err
	}
	job := &deploymentUpstartJob{
		name: strings.TrimSuffix(filepath.Base(path), ".conf"),
		user: "root",
		cwd:  "/",
	}
	section := ""
	mainScriptCmd := ""
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lower := strings.ToLower(line)
		if section != "" {
			if lower == "end script" {
				section = ""
				continue
			}
			if section == "script" && mainScriptCmd == "" {
				mainScriptCmd = deploymentParseUpstartScriptCommand(line)
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		keyword := strings.ToLower(fields[0])
		switch keyword {
		case "start":
			if len(fields) >= 2 && strings.ToLower(fields[1]) == "on" {
				job.hasStart = true
			}
		case "exec":
			if len(fields) >= 2 {
				job.exec = deploymentParseUpstartExec(strings.Join(fields[1:], " "))
			}
		case "setuid":
			if len(fields) >= 2 {
				job.user = strings.Trim(fields[1], `"'`)
			}
		case "chdir":
			if len(fields) >= 2 {
				job.cwd = strings.Trim(strings.Join(fields[1:], " "), `"'`)
			}
		case "script":
			section = "script"
		case "pre-start", "post-start", "pre-stop", "post-stop":
			if len(fields) >= 2 && strings.ToLower(fields[1]) == "script" {
				section = keyword
			}
		}
	}
	if job.exec == "" && mainScriptCmd != "" {
		job.exec = mainScriptCmd
	}

	// /etc/init/<job>.override 中的 manual 表示不自动启动
	overridePath := strings.TrimSuffix(path, ".conf") + ".override"
	if override, err := deploymentReadFileLimited(overridePath, deploymentMaxFileSize); err == nil {
		for _, line := range strings.Split(string(override), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if strings.EqualFold(line, "manual") {
				job.manual = true
			}
		}
	}
	return job, nil
}

func deploymentParseUpstartExec(value string) string {
	fields := strings.Fields(strings.TrimSpace(value))
	for i, field := range fields {
		if field == "--exec" && i+1 < len(fields) {
			return strings.Trim(fields[i+1], `"'`)
		}
	}
	return deploymentParseExecStart(value)
}

func deploymentParseUpstartScriptCommand(line string) string {
	lower := strings.ToLower(strings.TrimSpace(line))
	if lower == "" || strings.HasPrefix(lower, "#") {
		return ""
	}
	if strings.HasPrefix(lower, "exec ") {
		return deploymentParseUpstartExec(line[len("exec "):])
	}
	if strings.HasPrefix(lower, "start-stop-daemon") {
		return deploymentParseUpstartExec(line)
	}
	for _, skip := range []string{
		"[", "if ", "then", "else", "fi", "case ", "esac", "while ", "do ", "done",
		"export ", "umask ", "cd ", "set ", "trap ", "ulimit ", "nice ", "su ",
		"daemon ", "$", "}", "{", "start ", "stop ", "status ", "kill ", "echo ",
		"logger ", "sleep ", "test ", "which ", "command ",
	} {
		if strings.HasPrefix(lower, skip) {
			return ""
		}
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	candidate := strings.Trim(fields[0], `"'`)
	if strings.HasPrefix(candidate, "/") {
		return candidate
	}
	return ""
}

// deploymentUpstartStatus 通过 initctl list 获取 job 运行状态（老系统 initctl 在 /sbin）
func deploymentUpstartStatus(ctx context.Context) (map[string]string, bool) {
	status := make(map[string]string)
	if deploymentHostRoot() != "" {
		return status, false
	}
	bin := ""
	for _, candidate := range []string{"initctl", "/sbin/initctl", "/usr/sbin/initctl"} {
		if p, err := exec.LookPath(candidate); err == nil {
			bin = p
			break
		}
	}
	if bin == "" {
		return status, false
	}
	out, err := deploymentInventoryCommandOutput(ctx, 5*time.Second, 4<<20, bin, "list")
	if err != nil {
		return status, false
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		state := ""
		for _, field := range fields[1:] {
			if strings.Contains(field, "/") {
				state = strings.TrimSuffix(field, ",")
				break
			}
		}
		if state == "" {
			state = strings.TrimSuffix(fields[1], ",")
		}
		switch {
		case strings.HasPrefix(state, "start/"):
			status[name] = "running"
		case strings.HasPrefix(state, "stop/"):
			status[name] = "stopped"
		default:
			status[name] = strings.ToLower(state)
		}
	}
	return status, true
}

func deploymentCollectUpstart(ctx context.Context) ([]deploymentInventoryService, error) {
	initDir := deploymentHostPath("/etc/init")
	entries, err := os.ReadDir(initDir)
	if err != nil {
		return nil, nil // 非 Upstart 系统视为无数据
	}
	statusMap, statusKnown := deploymentUpstartStatus(ctx)

	var services []deploymentInventoryService
	for _, entry := range entries {
		if ctx.Err() != nil {
			return services, ctx.Err()
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".conf") {
			continue
		}
		jobName := strings.TrimSuffix(entry.Name(), ".conf")
		if deploymentIsSystemService(jobName) {
			continue
		}
		job, err := deploymentParseUpstartJob(filepath.Join(initDir, entry.Name()))
		if err != nil || job == nil {
			continue
		}
		middleware := deploymentDetectMiddleware(jobName, job.exec, "", "")
		if middleware == "none" && deploymentIsSystemExec(job.exec) {
			continue
		}

		enabled := "unknown"
		switch {
		case job.manual:
			enabled = "false"
		case job.hasStart:
			enabled = "true"
		default:
			enabled = "static"
		}

		status := "unknown"
		if state, ok := statusMap[jobName]; ok {
			status = state
		} else if statusKnown {
			status = "stopped"
		}

		path := job.exec
		if path == "" {
			path = filepath.Join(initDir, entry.Name())
		}
		cwd := job.cwd
		if cwd == "" {
			cwd = "/"
		}
		services = append(services, deploymentInventoryService{
			Name:        jobName,
			Path:        path,
			User:        job.user,
			StartupType: "upstart",
			Middleware:  middleware,
			Cwd:         cwd,
			Exe:         job.exec,
			Root:        "/",
			Status:      status,
			Enabled:     enabled,
			Source:      "upstart",
			dedupKey:    "upstart:" + jobName,
			instances:   0,
		})
	}
	if err := ctx.Err(); err != nil {
		return services, err
	}
	return services, nil
}

// ==========================================================================
// 数据源：supervisor
// ==========================================================================

func deploymentSupervisorConfigFiles() []string {
	candidates := []string{
		"/etc/supervisord.conf",
		"/etc/supervisor/supervisord.conf",
		"/etc/supervisor/conf.d/*.conf",
		"/etc/supervisord.d/*.conf",
		"/etc/supervisord.d/*.ini",
	}
	seen := make(map[string]struct{})
	var files []string
	for _, candidate := range candidates {
		matches, _ := filepath.Glob(deploymentHostPath(candidate))
		for _, match := range matches {
			if _, ok := seen[match]; ok {
				continue
			}
			seen[match] = struct{}{}
			files = append(files, match)
		}
	}
	return files
}

func deploymentCollectSupervisor(ctx context.Context) ([]deploymentInventoryService, error) {
	files := deploymentSupervisorConfigFiles()
	if len(files) == 0 {
		return nil, nil
	}

	// 解析 include 配置
	queue := append([]string(nil), files...)
	seen := make(map[string]struct{}, len(files))
	for len(queue) > 0 && len(seen) < 200 {
		path := queue[0]
		queue = queue[1:]
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		for _, include := range deploymentParseSupervisorIncludes(path) {
			if !filepath.IsAbs(include) {
				include = filepath.Join(filepath.Dir(path), include)
			}
			matches, _ := filepath.Glob(deploymentHostPath(strings.TrimPrefix(include, deploymentHostRoot())))
			for _, match := range matches {
				if _, ok := seen[match]; !ok {
					queue = append(queue, match)
				}
			}
		}
	}

	statusMap := deploymentSupervisorStatus(ctx)

	type programConfig = deploymentSupervisorProgram
	var programs []programConfig
	for path := range seen {
		if ctx.Err() != nil {
			break
		}
		parsed, err := deploymentParseSupervisorPrograms(path)
		if err != nil {
			continue
		}
		programs = append(programs, parsed...)
	}

	var services []deploymentInventoryService
	for _, p := range programs {
		if deploymentIsSystemService(p.name) {
			continue
		}
		exe := deploymentParseExecStart(p.command)
		cwd := p.directory
		if cwd == "" {
			cwd = "unknown"
		}
		userName := p.user
		if userName == "" || strings.Contains(userName, "%(") {
			userName = "unknown"
		}
		enabled := "true"
		if strings.EqualFold(p.autostart, "false") {
			enabled = "false"
		}
		status := "unknown"
		if state, ok := statusMap[p.name]; ok {
			status = state
		}
		services = append(services, deploymentInventoryService{
			Name:        p.name,
			Path:        exe,
			User:        userName,
			StartupType: "supervisor",
			Middleware:  deploymentDetectMiddleware(p.name, exe, p.command, ""),
			Cwd:         cwd,
			Exe:         exe,
			Root:        "/",
			Status:      status,
			Enabled:     enabled,
			Source:      "supervisor",
			dedupKey:    "supervisor:" + p.name,
			instances:   0,
		})
	}
	if err := ctx.Err(); err != nil {
		return services, err
	}
	return services, nil
}

func deploymentParseSupervisorIncludes(path string) []string {
	data, err := deploymentReadFileLimited(path, deploymentMaxFileSize)
	if err != nil {
		return nil
	}
	var includes []string
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}
		if section != "include" {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		if strings.ToLower(strings.TrimSpace(line[:idx])) != "files" {
			continue
		}
		for _, f := range strings.Fields(line[idx+1:]) {
			includes = append(includes, f)
		}
	}
	return includes
}

type deploymentSupervisorProgram struct {
	name      string
	command   string
	directory string
	user      string
	autostart string
}

func deploymentParseSupervisorPrograms(path string) ([]deploymentSupervisorProgram, error) {
	data, err := deploymentReadFileLimited(path, deploymentMaxFileSize)
	if err != nil {
		return nil, err
	}
	var programs []deploymentSupervisorProgram
	var current *deploymentSupervisorProgram
	section := ""
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.Trim(line, "[]"))
			current = nil
			if strings.HasPrefix(section, "program:") || strings.HasPrefix(section, "fcgi-program:") ||
				strings.HasPrefix(section, "eventlistener:") {
				idx := strings.Index(section, ":")
				if idx >= 0 && idx+1 < len(section) {
					programs = append(programs, deploymentSupervisorProgram{name: strings.TrimSpace(section[idx+1:])})
					current = &programs[len(programs)-1]
				}
			}
			continue
		}
		if current == nil {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(line[:idx]))
		value := strings.TrimSpace(line[idx+1:])
		switch key {
		case "command":
			current.command = value
		case "directory":
			current.directory = value
		case "user":
			current.user = value
		case "autostart":
			current.autostart = value
		}
	}
	return programs, scanner.Err()
}

func deploymentSupervisorStatus(ctx context.Context) map[string]string {
	status := make(map[string]string)
	if deploymentHostRoot() != "" {
		return status
	}
	if _, err := exec.LookPath("supervisorctl"); err != nil {
		return status
	}
	out, err := deploymentInventoryCommandOutput(ctx, 5*time.Second, 4<<20, "supervisorctl", "status")
	if err != nil {
		return status
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		state := strings.ToLower(fields[1])
		switch state {
		case "running":
			status[fields[0]] = "running"
		case "stopped", "exited", "fatal", "backoff", "starting", "stopping":
			status[fields[0]] = state
		}
	}
	return status
}

// ==========================================================================
// 数据源：进程
// ==========================================================================

// deploymentProcessInfo 单个进程的原始信息
type deploymentProcessInfo struct {
	pid       int
	name      string
	exe       string
	cwd       string
	root      string
	cmdline   string
	uid       int
	user      string
	cgroup    string
	unit      string
	container string
}

// deploymentProcLinkMode 返回 /proc/<pid>/{exe,cwd,root} 读取策略：
// all=全部读取；none=不读取；auto=发现 NFS/Ceph 等网络文件系统时跳过（默认）。
func deploymentProcLinkMode() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DEPLOYMENT_INVENTORY_PROC_LINKS"))) {
	case "all":
		return "all"
	case "none":
		return "none"
	default:
		return "auto"
	}
}

var deploymentNetworkFSTypes = map[string]struct{}{
	"nfs": {}, "nfs4": {}, "ceph": {}, "cephfs": {},
	"cifs": {}, "smb3": {}, "glusterfs": {}, "lustre": {},
	"9p": {}, "afs": {}, "davfs": {}, "davfs2": {}, "ncpfs": {},
	"gpfs": {}, "beegfs": {}, "juicefs": {}, "alluxio": {},
	"cvmfs": {}, "s3fs": {}, "ocfs2": {}, "gfs2": {},
	"fuse.sshfs": {}, "fuse.s3fs": {}, "fuse.ceph": {},
	"fuse.glusterfs": {}, "fuse.juicefs": {}, "fuse.alluxio": {},
	"fuse.gcsfuse": {}, "fuse.rclone": {}, "fuse.curlftpfs": {},
	"fuse.davfs2": {}, "fuse.s3ql": {},
}

// deploymentIsNetworkFSType 判断挂载类型是否可能因网络存储挂死
func deploymentIsNetworkFSType(fstype string) bool {
	_, ok := deploymentNetworkFSTypes[strings.ToLower(strings.TrimSpace(fstype))]
	return ok
}

// deploymentProcessHasNetworkFS 检查进程 mount namespace 是否包含网络文件系统
func deploymentProcessHasNetworkFS(procRoot string, pid int) bool {
	base := filepath.Join(procRoot, strconv.Itoa(pid))
	nsID := ""
	if link, err := os.Readlink(filepath.Join(base, "ns", "mnt")); err == nil {
		nsID = link
		if cached, ok := deploymentNetworkFSCache.Load(nsID); ok {
			return cached.(bool)
		}
	}

	data, err := deploymentReadFileLimited(filepath.Join(base, "mountinfo"), 512*1024)
	if err != nil {
		return false
	}
	hasNetworkFS := false
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if separator >= 0 && separator+1 < len(fields) && deploymentIsNetworkFSType(fields[separator+1]) {
			hasNetworkFS = true
			break
		}
	}

	// 缓存同一 mount namespace 的结果，避免每个进程重复解析数百 KB 的 mountinfo。
	// 限制缓存条目数，防止异常环境创建大量 namespace 导致内存增长。
	if nsID != "" && deploymentNetworkFSCacheCount.Load() < 4096 {
		if _, loaded := deploymentNetworkFSCache.LoadOrStore(nsID, hasNetworkFS); !loaded {
			deploymentNetworkFSCacheCount.Add(1)
		}
	}
	return hasNetworkFS
}

func deploymentExeFromCmdline(cmdline string) string {
	fields := strings.Fields(cmdline)
	if len(fields) == 0 {
		return ""
	}
	first := strings.Trim(fields[0], `"'`)
	if strings.HasPrefix(first, "/") {
		return deploymentCleanProcPath(first)
	}
	return ""
}

func deploymentReadProcess(procRoot string, pid int, skipContainer bool, linkMode string) (*deploymentProcessInfo, bool) {
	base := filepath.Join(procRoot, strconv.Itoa(pid))
	info := &deploymentProcessInfo{pid: pid}

	// 先读 cgroup：容器内进程默认在读取 comm/cmdline/mountinfo/readlink 之前就跳过，
	// 避免 Kubernetes/Docker 节点上成千上万个容器进程造成额外 I/O 和卡死风险。
	if data, err := deploymentReadFileLimited(filepath.Join(base, "cgroup"), 64*1024); err == nil {
		info.cgroup = string(data)
		info.unit, info.container = deploymentParseCgroup(string(data))
	}
	if skipContainer && info.container != "" {
		return nil, false
	}

	if data, err := deploymentReadFileLimited(filepath.Join(base, "comm"), 4096); err == nil {
		info.name = strings.TrimSpace(string(data))
	}
	if data, err := deploymentReadFileLimited(filepath.Join(base, "cmdline"), 64*1024); err == nil {
		info.cmdline = strings.TrimSpace(strings.ReplaceAll(string(data), "\x00", " "))
	}

	// 默认 auto：进程挂载了 NFS/Ceph 等网络文件系统时，不读取 exe/cwd/root 链接，
	// 避免 readlink 进入挂死的网络存储导致采集线程卡在 D 状态。
	linksAllowed := true
	switch linkMode {
	case "none":
		linksAllowed = false
	case "all":
		linksAllowed = true
	default:
		linksAllowed = !deploymentProcessHasNetworkFS(procRoot, pid)
	}
	if linksAllowed {
		if link, err := os.Readlink(filepath.Join(base, "exe")); err == nil {
			info.exe = deploymentCleanProcPath(link)
		}
		if link, err := os.Readlink(filepath.Join(base, "cwd")); err == nil {
			info.cwd = deploymentCleanProcPath(link)
		}
		if link, err := os.Readlink(filepath.Join(base, "root")); err == nil {
			info.root = deploymentCleanProcPath(link)
		}
	}
	if info.exe == "" {
		// 跳过 readlink 时仍可从 cmdline 获取可执行文件路径（只读 /proc，不触碰业务存储）
		info.exe = deploymentExeFromCmdline(info.cmdline)
	}
	if data, err := deploymentReadFileLimited(filepath.Join(base, "status"), 64*1024); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "Uid:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					if uid, err := strconv.Atoi(fields[1]); err == nil {
						info.uid = uid
					}
				}
				break
			}
		}
	}

	if info.name == "" {
		info.name = filepath.Base(info.exe)
	}
	info.name = deploymentSanitizeLabel(info.name)
	if info.name == "" && info.exe == "unknown" && info.cmdline == "" {
		return nil, false
	}
	info.user = deploymentUserName(info.uid)
	return info, true
}

// deploymentParseCgroup 解析 cgroup，返回 systemd service 单元和容器标识
func deploymentParseCgroup(cgroup string) (unit, container string) {
	for _, line := range strings.Split(cgroup, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 3)
		path := parts[len(parts)-1]
		lower := strings.ToLower(path)
		switch {
		case strings.Contains(lower, "/kubepods") || strings.Contains(lower, "kubepods-") ||
			strings.Contains(lower, "cri-containerd-") || strings.Contains(lower, "crio-"):
			container = "containerd"
		case strings.Contains(lower, "/docker/") || strings.Contains(lower, "docker-") ||
			strings.Contains(lower, "libpod-") || strings.Contains(lower, "/libpod/"):
			container = "docker"
		}
		segments := strings.Split(path, "/")
		for _, seg := range segments {
			seg = strings.TrimSpace(seg)
			if strings.HasSuffix(seg, ".service") {
				unit = seg
			}
		}
	}
	return unit, container
}

func deploymentSanitizeLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' || r == '"' {
			return ' '
		}
		return r
	}, value)
}

// deploymentShouldSkipContainerProcess 容器 cgroup 内进程默认全部跳过，
// 避免 Docker/Kubernetes 容器数量多时产生高基数和运行时卡死。
func deploymentShouldSkipContainerProcess() bool {
	// 容器清单/容器内进程可能带来高基数和运行时卡死风险，默认全部跳过。
	// 如确需采集容器内进程，可设置 DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES=true。
	return !deploymentEnvBool("DEPLOYMENT_INVENTORY_INCLUDE_CONTAINER_PROCESSES", false)
}

// deploymentCollectProcesses 并发扫描 /proc，构建进程级部署清单
func deploymentCollectProcesses(ctx context.Context) ([]deploymentInventoryService, error) {
	procRoot := deploymentProcRoot()
	procDir, err := os.Open(procRoot)
	if err != nil {
		return nil, fmt.Errorf("open %s failed: %w", procRoot, err)
	}
	defer procDir.Close()

	// 每次扫描重建 mount namespace 缓存，避免业务后续挂载 NFS/Ceph 后仍使用旧的 false 结果。
	deploymentNetworkFSCache.Clear()
	deploymentNetworkFSCacheCount.Store(0)

	skipContainer := deploymentShouldSkipContainerProcess()
	linkMode := deploymentProcLinkMode()
	concurrency := deploymentInventoryConcurrency()
	jobs := make(chan int, concurrency*2)
	var closeOnce sync.Once
	closeJobs := func() {
		closeOnce.Do(func() { close(jobs) })
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	groups := make(map[string]*deploymentInventoryService)

	// 兜底：无论正常返回、ctx 取消还是 panic，都关闭 jobs 并等待 worker，
	// 避免 worker 永久阻塞在 jobs 上造成 goroutine 泄漏。
	defer func() {
		closeJobs()
		wg.Wait()
	}()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[deployment_inventory] process worker panic recovered: %v", r)
				}
			}()
			for pid := range jobs {
				if ctx.Err() != nil {
					return
				}
				info, ok := deploymentReadProcess(procRoot, pid, skipContainer, linkMode)
				if !ok || info == nil {
					continue
				}
				svc, keep := deploymentBuildProcessService(info, skipContainer)
				if !keep || svc == nil {
					continue
				}
				key := svc.dedupKey
				func() {
					mu.Lock()
					defer mu.Unlock()
					if existing, ok := groups[key]; ok {
						existing.instances++
						// 补全缺失信息
						if existing.Cwd == "unknown" && svc.Cwd != "unknown" {
							existing.Cwd = svc.Cwd
						}
						if existing.Exe == "unknown" && svc.Exe != "unknown" {
							existing.Exe = svc.Exe
						}
						if existing.Middleware == "none" && svc.Middleware != "none" {
							existing.Middleware = svc.Middleware
						}
					} else {
						copySvc := *svc
						copySvc.instances = 1
						groups[key] = &copySvc
					}
				}()
			}
		}()
	}

	// 流式读取 /proc，避免一次性把所有 PID 放入大切片；进程极多时降低峰值内存。
	var readErr error
sendLoop:
	for {
		entries, err := procDir.ReadDir(512)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 2 {
				continue
			}
			select {
			case jobs <- pid:
			case <-ctx.Done():
				break sendLoop
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
	}
	closeJobs()
	wg.Wait()

	if ctx.Err() != nil {
		return deploymentProcessGroupsToSlice(groups), ctx.Err()
	}
	if readErr != nil {
		return deploymentProcessGroupsToSlice(groups), fmt.Errorf("read %s failed: %w", procRoot, readErr)
	}
	return deploymentProcessGroupsToSlice(groups), nil
}

func deploymentProcessGroupsToSlice(groups map[string]*deploymentInventoryService) []deploymentInventoryService {
	out := make([]deploymentInventoryService, 0, len(groups))
	for _, svc := range groups {
		out = append(out, *svc)
	}
	return out
}

func deploymentIsUserSessionCgroup(cgroup string) bool {
	return strings.Contains(cgroup, "/user.slice/") || strings.Contains(cgroup, "user@")
}

// deploymentBuildProcessService 将进程信息转换为服务条目，返回 keep=false 表示排除
func deploymentBuildProcessService(info *deploymentProcessInfo, skipContainer bool) (*deploymentInventoryService, bool) {
	if info.pid <= 2 || info.pid == os.Getpid() {
		return nil, false
	}
	if info.cmdline == "" && (info.exe == "" || info.exe == "unknown") {
		return nil, false // 内核线程
	}
	if skipContainer && info.container != "" {
		return nil, false
	}
	if deploymentIsSystemPath(info.exe) {
		return nil, false
	}
	if deploymentIsSystemService(info.name) || deploymentIsInteractiveProcess(info.name) {
		return nil, false
	}
	if info.exe != "" && info.exe != "unknown" {
		exeBase := filepath.Base(info.exe)
		if deploymentIsSystemService(exeBase) || deploymentIsInteractiveProcess(exeBase) {
			return nil, false
		}
	}
	if info.unit != "" && deploymentIsSystemService(info.unit) {
		return nil, false
	}

	name := deploymentDeriveProcessName(info.name, info.cmdline, info.exe)
	if name == "" {
		name = info.name
	}
	if name == "" {
		return nil, false
	}

	exe := info.exe
	if exe == "" {
		exe = "unknown"
	}
	cwd := info.cwd
	if cwd == "" {
		cwd = "unknown"
	}
	root := info.root
	if root == "" {
		root = "unknown"
	}

	startupType := "process"
	key := ""
	unitBase := strings.TrimSuffix(info.unit, ".service")
	isAggregatorUnit := unitBase == "supervisord" || unitBase == "supervisor" ||
		unitBase == "runsvdir" || unitBase == "s6-svscan"
	if info.unit != "" && !isAggregatorUnit {
		key = "systemd:" + info.unit
		startupType = "systemd"
		if name == "" {
			name = unitBase
		}
	} else {
		key = "process:" + deploymentNormalizeServiceName(name) + "|" + exe + "|" + cwd + "|" + info.user
	}

	middleware := deploymentDetectMiddleware(name, exe, info.cmdline, "")
	// 开发工具/编辑器/桌面辅助进程不属于部署清单
	if middleware == "none" && deploymentIsToolingProcess(name, exe, info.cmdline) {
		return nil, false
	}
	// 可通过 DEPLOYMENT_INVENTORY_INCLUDE_USER_PROCESSES=false 排除用户登录会话进程
	if middleware == "none" && deploymentIsUserSessionCgroup(info.cgroup) &&
		!deploymentEnvBool("DEPLOYMENT_INVENTORY_INCLUDE_USER_PROCESSES", true) {
		return nil, false
	}

	return &deploymentInventoryService{
		Name:        name,
		Path:        exe,
		User:        info.user,
		StartupType: startupType,
		Middleware:  middleware,
		Cwd:         cwd,
		Exe:         exe,
		Root:        root,
		Status:      "running",
		Enabled:     "unknown",
		Source:      "process",
		dedupKey:    key,
		instances:   1,
	}, true
}

// deploymentDeriveProcessName 从命令行推导更友好的服务名（如 java -jar app.jar -> app）
func deploymentDeriveProcessName(comm, cmdline, exe string) string {
	fields := strings.Fields(deploymentCommandTextForScan(cmdline))
	for i := 0; i < len(fields)-1; i++ {
		if fields[i] == "-jar" {
			base := filepath.Base(strings.Trim(fields[i+1], `"'`))
			base = strings.TrimSuffix(base, ".jar")
			base = strings.TrimSuffix(base, ".war")
			if base != "" {
				return deploymentSanitizeLabel(base)
			}
		}
	}
	// Linux comm 最长 15 字符，过长时优先使用可执行文件名，避免服务名被截断
	if exe != "" && exe != "unknown" && (len(comm) >= 15 || comm == "") {
		if base := deploymentSanitizeLabel(filepath.Base(exe)); base != "" {
			return base
		}
	}
	if comm != "" {
		return deploymentSanitizeLabel(comm)
	}
	if exe != "" && exe != "unknown" {
		return deploymentSanitizeLabel(filepath.Base(exe))
	}
	return ""
}

// ==========================================================================
// Prometheus Collector
// ==========================================================================

// DeploymentInventoryCollector 主机部署清单采集器
type DeploymentInventoryCollector struct {
	serviceInfoDesc       *prometheus.Desc
	servicesTotalDesc     *prometheus.Desc
	lastRefreshDesc       *prometheus.Desc
	refreshDurationDesc   *prometheus.Desc
	refreshErrorsDesc     *prometheus.Desc
	readyDesc             *prometheus.Desc
	sourceUpDesc          *prometheus.Desc
	sourceLastSuccessDesc *prometheus.Desc
}

// NewDeploymentInventoryCollector 创建部署清单采集器，并启动异步刷新协程
func NewDeploymentInventoryCollector() *DeploymentInventoryCollector {
	startDeploymentInventoryWorker()
	return &DeploymentInventoryCollector{
		serviceInfoDesc: prometheus.NewDesc(
			"node_deployment_inventory_info",
			"主机已安装部署的服务/中间件清单（值恒为1）",
			[]string{
				"service_name", "service_path", "service_user", "startup_type",
				"middleware", "cwd", "exe", "root", "status", "enabled", "source",
			}, nil,
		),
		servicesTotalDesc: prometheus.NewDesc(
			"node_deployment_inventory_services_total",
			"主机部署清单中的服务/中间件总数", nil, nil,
		),
		lastRefreshDesc: prometheus.NewDesc(
			"node_deployment_inventory_last_refresh_timestamp_seconds",
			"部署清单最后一次刷新完成的 Unix 时间戳", nil, nil,
		),
		refreshDurationDesc: prometheus.NewDesc(
			"node_deployment_inventory_refresh_duration_seconds",
			"部署清单最后一次刷新耗时（秒）", nil, nil,
		),
		refreshErrorsDesc: prometheus.NewDesc(
			"node_deployment_inventory_refresh_errors_total",
			"部署清单刷新过程中累计失败的数据源次数", nil, nil,
		),
		readyDesc: prometheus.NewDesc(
			"node_deployment_inventory_ready",
			"部署清单是否已完成首次加载（1=已就绪，0=加载中）", nil, nil,
		),
		sourceUpDesc: prometheus.NewDesc(
			"node_deployment_inventory_source_up",
			"部署清单各数据源最近一次刷新是否成功（1=成功，0=失败；失败时在缓存 TTL 内沿用上次结果）",
			[]string{"source"}, nil,
		),
		sourceLastSuccessDesc: prometheus.NewDesc(
			"node_deployment_inventory_source_last_success_timestamp_seconds",
			"部署清单各数据源最后一次采集成功的 Unix 时间戳（0=从未成功）",
			[]string{"source"}, nil,
		),
	}
}

// Describe 实现 prometheus.Collector 接口
func (c *DeploymentInventoryCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.serviceInfoDesc
	ch <- c.servicesTotalDesc
	ch <- c.lastRefreshDesc
	ch <- c.refreshDurationDesc
	ch <- c.refreshErrorsDesc
	ch <- c.readyDesc
	ch <- c.sourceUpDesc
	ch <- c.sourceLastSuccessDesc
}

// Collect 实现 prometheus.Collector 接口：只读取内存快照，不执行任何阻塞操作
func (c *DeploymentInventoryCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot := deploymentInventorySnapshotPtr.Load()
	if snapshot == nil {
		ch <- prometheus.MustNewConstMetric(c.readyDesc, prometheus.GaugeValue, 0)
		ch <- prometheus.MustNewConstMetric(c.servicesTotalDesc, prometheus.GaugeValue, 0)
		ch <- prometheus.MustNewConstMetric(c.lastRefreshDesc, prometheus.GaugeValue, 0)
		ch <- prometheus.MustNewConstMetric(c.refreshDurationDesc, prometheus.GaugeValue, 0)
		ch <- prometheus.MustNewConstMetric(c.refreshErrorsDesc, prometheus.CounterValue, float64(deploymentInventoryRefreshErrors.Load()))
		return
	}

	ch <- prometheus.MustNewConstMetric(c.readyDesc, prometheus.GaugeValue, 1)

	for _, svc := range snapshot.services {
		ch <- prometheus.MustNewConstMetric(
			c.serviceInfoDesc,
			prometheus.GaugeValue,
			1,
			svc.Name,
			svc.Path,
			svc.User,
			svc.StartupType,
			svc.Middleware,
			svc.Cwd,
			svc.Exe,
			svc.Root,
			svc.Status,
			svc.Enabled,
			svc.Source,
		)
	}
	ch <- prometheus.MustNewConstMetric(c.servicesTotalDesc, prometheus.GaugeValue, float64(len(snapshot.services)))
	ch <- prometheus.MustNewConstMetric(c.lastRefreshDesc, prometheus.GaugeValue, float64(snapshot.lastRefresh.Unix()))
	ch <- prometheus.MustNewConstMetric(c.refreshDurationDesc, prometheus.GaugeValue, snapshot.duration.Seconds())
	ch <- prometheus.MustNewConstMetric(c.refreshErrorsDesc, prometheus.CounterValue, float64(deploymentInventoryRefreshErrors.Load()))

	for source, up := range snapshot.sourceUp {
		ch <- prometheus.MustNewConstMetric(c.sourceUpDesc, prometheus.GaugeValue, up, source)
	}
	for source, last := range snapshot.sourceLastSuccess {
		ch <- prometheus.MustNewConstMetric(c.sourceLastSuccessDesc, prometheus.GaugeValue, float64(last.Unix()), source)
	}
}

// Update 实现 node_exporter 自定义 Collector 接口
func (c *DeploymentInventoryCollector) Update(ch chan<- prometheus.Metric) error {
	c.Collect(ch)
	return nil
}
