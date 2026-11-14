package my_collectors

import "testing"

func TestDeploymentServiceIdentity(t *testing.T) {
	cases := map[string]string{
		"nginx":         "nginx",
		"nginx.service": "nginx",
		"NGINX":         "nginx",
		"myapp.sh":      "myapp",
		"nginx@blue":    "nginx@blue",
		"nginx@green":   "nginx@green",
	}
	for input, expected := range cases {
		if got := deploymentServiceIdentity(input); got != expected {
			t.Fatalf("identity(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestDeploymentMergeServicesDeduplicatesSameName(t *testing.T) {
	input := []deploymentInventoryService{
		{
			Name: "nginx", Path: "/usr/sbin/nginx", User: "root",
			StartupType: "systemd", Middleware: "nginx", Exe: "/usr/sbin/nginx",
			Status: "running", Enabled: "true", Source: "systemd",
			dedupKey: "systemd:nginx.service",
		},
		{
			Name: "nginx", Path: "/usr/sbin/nginx", User: "root",
			StartupType: "systemd", Middleware: "none", Cwd: "/etc/nginx",
			Exe: "/usr/sbin/nginx", Status: "running", Enabled: "unknown",
			Source: "process", dedupKey: "systemd:nginx.service", instances: 1,
		},
		{
			Name: "redis", Path: "/usr/bin/redis-server", User: "redis",
			StartupType: "systemd", Middleware: "redis", Exe: "/usr/bin/redis-server",
			Status: "running", Enabled: "true", Source: "systemd",
			dedupKey: "systemd:redis.service",
		},
	}
	out := deploymentMergeServices(input)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique services, got %d: %+v", len(out), out)
	}
	var nginx *deploymentInventoryService
	for i := range out {
		if out[i].Name == "nginx" {
			nginx = &out[i]
		}
	}
	if nginx == nil {
		t.Fatal("nginx missing")
	}
	if nginx.Cwd != "/etc/nginx" || nginx.Middleware != "nginx" || nginx.StartupType != "systemd" {
		t.Fatalf("unexpected merged nginx: %+v", nginx)
	}
}

func TestDeploymentIsNetworkFSType(t *testing.T) {
	for _, fs := range []string{"nfs", "nfs4", "ceph", "cifs", "glusterfs", "fuse.sshfs", "lustre", "9p", "ocfs2"} {
		if !deploymentIsNetworkFSType(fs) {
			t.Fatalf("%q should be detected as network fs", fs)
		}
	}
	for _, fs := range []string{"ext4", "xfs", "btrfs", "overlay", "tmpfs", "proc", "fusectl", "fuse.lxcfs", "fuse.gvfsd-fuse"} {
		if deploymentIsNetworkFSType(fs) {
			t.Fatalf("%q should not be detected as network fs", fs)
		}
	}
}

func TestDeploymentParseCgroup(t *testing.T) {
	cases := []struct {
		cgroup    string
		container string
	}{
		{"0::/kubepods.slice/kubepods-burstable.slice/cri-containerd-abc.scope", "containerd"},
		{"0::/kubepods/burstable/pod123/abc", "containerd"},
		{"0::/system.slice/docker-abc.scope", "docker"},
		{"0::/system.slice/nginx.service", ""},
	}
	for _, c := range cases {
		_, container := deploymentParseCgroup(c.cgroup)
		if container != c.container {
			t.Fatalf("cgroup %q: expected container %q, got %q", c.cgroup, c.container, container)
		}
	}
}
