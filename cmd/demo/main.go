package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/client/llb"
	"github.com/moby/buildkit/util/entitlements"
	"github.com/moby/buildkit/util/progress/progresswriter"
)

func nilordie(err error) {
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
}

const progressMode = "tty"

func main() {
	ctx := context.Background()
	c, err := client.New(ctx, "unix:///run/buildkit/buildkitd.sock")
	nilordie(err)
	defer c.Close()

	buildBase := llb.Image("busybox:latest")

	runcVersions := []string{"1.0.0", "1.0.1"}
	var runcs []llb.State
	for _, v := range runcVersions {
		runcs = append(runcs, buildRunc(ctx, c, buildBase, v))
	}
	containerd := buildContainerd(ctx, c, buildBase)
	kubernetes := buildKubernetes(ctx, c, buildBase)

	// invalidate previous runs of the next steps without invalidating previous steps or disabling the cache
	base := llb.Image("debian:latest").File(llb.Mkfile("/."+strconv.Itoa(int(time.Now().UnixNano())), 0444, nil))
	def, err := base.Marshal(ctx)
	nilordie(err)
	pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
	nilordie(err)
	_, err = c.Solve(ctx, def, client.SolveOpt{
		AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
		Exports: []client.ExportEntry{{
			Type: client.ExporterImage,
			Attrs: map[string]string{
				"name": "127.0.0.1:5000/demo-base:latest",
				"push": "true",
			},
		}},
	}, pw.Status())
	nilordie(err)

	/*
		Scenario:
		* Creating and exporting a container image that combines a base image with 3 separately built artifacts:
		  runc, containerd and kubernetes. It matches what you'd typically do in a multi-stage Dockerfile build.

		* The performance of a Copy approach is compared with a MergeOp approach.

		* To ensure the comparison is fair, the base image and all of the artifacts are already cached before the
		  tests are run.

		* Two builds are run for each approach, each one has a different version of runc but the same versions of
		  containerd and kubernetes.
	*/

	time.Sleep(time.Second)
	fmt.Printf("\n\n\n")
	fmt.Printf("Press enter to run Copy-Op demo...\n")
	fmt.Scanln()
	fmt.Printf("\n\n\n")

	fmt.Printf("#############################\n")
	fmt.Printf("####### CopyOp\n")
	fmt.Printf("#############################\n")

	/*
		Copy-based approach:
		* Start with the base image and copy the artifacts on top of it and export the final image. Basically:
		  FROM build-base as runc
		  RUN ...

		  FROM build-base as containerd
		  RUN ...

		  FROM build-base as kubernetes
		  RUN ...

		  FROM base
		  COPY --from=runc / /
		  COPY --from=containerd / /
		  COPY --from=kubernetes / /

		* Copying takes time (especially for Kubernetes which has 100+ MB of artifacts). It's also ~O(N) where
		  N is the size of the artifacts.

		* When the version of runc is changed, it invalidates the next copy steps and thus results in extra
		  time and disk space being spent.

		* Exports take a long time because even though the artifacts are already cached, the act of copying them
		  creates distinct layers with new chain IDs and thus requires new (mostly identical) layers to be
		  exported.
	*/

	var copyTime time.Duration
	func() {
		start := time.Now()
		defer func() {
			done := time.Now()
			copyTime = done.Sub(start)
		}()

		for i, runc := range runcs {
			version := runcVersions[i]
			install := base.File(llb.Copy(runc, "/", "/"), llb.WithCustomName("copy runc "+version))
			install = install.File(llb.Copy(containerd, "/", "/"), llb.WithCustomName("copy containerd"))
			install = install.File(llb.Copy(kubernetes, "/", "/"), llb.WithCustomName("copy kubernetes"))
			install = install.Run(llb.Args([]string{"bash", "-c", "-e", strings.Join([]string{
				fmt.Sprintf("[[ \"$(runc --version | head -n1 | cut -d' ' -f3)\" == \"%s\" ]]", version),
				"runc --version",
				"containerd --version",
				"kubelet --version",
			}, " && ")})).Root()

			def, err := install.Marshal(ctx)
			nilordie(err)

			pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
			nilordie(err)

			_, err = c.Solve(ctx, def, client.SolveOpt{
				AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
				Exports: []client.ExportEntry{{
					Type: client.ExporterImage,
					Attrs: map[string]string{
						"name": "127.0.0.1:5000/copy-demo:" + version,
						"push": "true",
					},
				}},
			}, pw.Status())
			nilordie(err)
		}
	}()

	time.Sleep(time.Second)
	fmt.Printf("\n\n\n")
	fmt.Printf("Copy-op time: %s\n", copyTime)
	fmt.Printf("Press enter to run Merge-Op demo...\n")
	fmt.Scanln()
	fmt.Printf("\n\n\n")

	fmt.Printf("#############################\n")
	fmt.Printf("####### MergeOp\n")
	fmt.Printf("#############################\n")

	/*
		MergeOp approach:
		* Just merge the artifacts on top of the base image, which is highly efficient because an overlay
		  snapshotter is being used.

		* Merging takes almost no time. It's practically speaking O(1) because performing the merge is just
		  a matter of doing string manipulations in memeory.

		* When the version of runc changes, even though the MergeOp is technically invalidated, it doesn't
		  result in any extra work because there's close to zero cost to performing the Merge anyways.

		* Exports are very fast because no new layers are created by the MergeOp, the previously cached
		  layers for each artifact are just exported directly.
	*/

	var mergeTime time.Duration
	func() {
		start := time.Now()
		defer func() {
			done := time.Now()
			mergeTime = done.Sub(start)
		}()

		for i, runc := range runcs {
			version := runcVersions[i]
			install := llb.Merge([]llb.State{base, runc, containerd, kubernetes}, llb.WithCustomNamef("merge runc %s, container and kubernetes", version))
			install = install.Run(llb.Args([]string{"bash", "-c", "-e", strings.Join([]string{
				fmt.Sprintf("[[ \"$(runc --version | head -n1 | cut -d' ' -f3)\" == \"%s\" ]]", version),
				"containerd --version",
				"kubelet --version",
			}, " && ")})).Root()

			def, err := install.Marshal(ctx)
			nilordie(err)

			pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
			nilordie(err)

			_, err = c.Solve(ctx, def, client.SolveOpt{
				AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
				Exports: []client.ExportEntry{{
					Type: client.ExporterImage,
					Attrs: map[string]string{
						"name": "127.0.0.1:5000/merge-demo:" + version,
						"push": "true",
					},
				}},
			}, pw.Status())
			nilordie(err)
		}
	}()

	time.Sleep(time.Second)
	fmt.Printf("\n\n\n")
	fmt.Printf("######################################\n")
	fmt.Printf("####### Copy-op time: %s\n", copyTime)
	fmt.Printf("####### Merge-op time: %s\n", mergeTime)
	fmt.Printf("######################################\n")
	fmt.Printf("Press enter to end demo...\n")
	fmt.Scanln()
	fmt.Printf("\n\n\n")
}

func buildRunc(ctx context.Context, c *client.Client, base llb.State, version string) llb.State {
	runc := base.Run(llb.Args([]string{"sh", "-c", strings.Join([]string{
		fmt.Sprintf("wget https://github.com/opencontainers/runc/releases/download/v%s/runc.amd64", version),
		"mkdir -p /install/usr/bin",
		"install -m0777 runc.amd64 /install/usr/bin/runc",
	}, " && ")}), llb.Network(llb.NetModeHost)).AddMount("/install", llb.Scratch())

	pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
	nilordie(err)

	def, err := runc.Marshal(ctx)
	nilordie(err)
	_, err = c.Solve(ctx, def, client.SolveOpt{
		AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
		Exports: []client.ExportEntry{{
			Type: client.ExporterImage,
			Attrs: map[string]string{
				"name": "127.0.0.1:5000/demo-runc:" + version,
				"push": "true",
			},
		}},
	}, pw.Status())
	nilordie(err)

	return runc
}

func buildContainerd(ctx context.Context, c *client.Client, base llb.State) llb.State {
	containerd := base.Run(llb.Args([]string{"sh", "-c", strings.Join([]string{
		"wget https://github.com/containerd/containerd/releases/download/v1.5.5/cri-containerd-cni-1.5.5-linux-amd64.tar.gz",
		"tar x -z -f cri-containerd-cni-1.5.5-linux-amd64.tar.gz -C /install",
		"rm /install/usr/local/sbin/runc",
	}, " && ")}), llb.Network(llb.NetModeHost)).AddMount("/install", llb.Scratch())

	pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
	nilordie(err)

	def, err := containerd.Marshal(ctx)
	nilordie(err)
	_, err = c.Solve(ctx, def, client.SolveOpt{
		AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
		Exports: []client.ExportEntry{{
			Type: client.ExporterImage,
			Attrs: map[string]string{
				"name": "127.0.0.1:5000/demo-containerd:latest",
				"push": "true",
			},
		}},
	}, pw.Status())
	nilordie(err)

	return containerd
}

func buildKubernetes(ctx context.Context, c *client.Client, base llb.State) llb.State {
	kubernetes := base.Run(llb.Args([]string{"sh", "-c", strings.Join([]string{
		"wget https://dl.k8s.io/v1.22.0/kubernetes-server-linux-amd64.tar.gz",
		"tar x -z -f kubernetes-server-linux-amd64.tar.gz -C /install --strip-components 2 kubernetes/server/bin",
	}, " && ")}), llb.Network(llb.NetModeHost)).AddMount("/install", llb.Scratch())

	pw, err := progresswriter.NewPrinter(ctx, os.Stdout, progressMode)
	nilordie(err)

	def, err := kubernetes.Marshal(ctx)
	nilordie(err)
	_, err = c.Solve(ctx, def, client.SolveOpt{
		AllowedEntitlements: []entitlements.Entitlement{entitlements.EntitlementNetworkHost},
		Exports: []client.ExportEntry{{
			Type: client.ExporterImage,
			Attrs: map[string]string{
				"name": "127.0.0.1:5000/demo-kubernetes:latest",
				"push": "true",
			},
		}},
	}, pw.Status())
	nilordie(err)

	return kubernetes
}
