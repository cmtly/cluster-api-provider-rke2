//go:build e2e
// +build e2e

/*
Copyright 2026 SUSE.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	bootstrapv1 "github.com/rancher/cluster-api-provider-rke2/bootstrap/api/v1beta2"
	controlplanev1 "github.com/rancher/cluster-api-provider-rke2/controlplane/api/v1beta2"
)

const (
	// watchFilterFlag is the manager flag restricting the objects reconciled by a provider instance.
	watchFilterFlag = "--watch-filter="
	// watchFilterInstanceLabel tells apart the Pods of the additional provider instances.
	watchFilterInstanceLabel = "e2e.caprke2.cluster.x-k8s.io/instance"
	// watchFilterVariable is the template variable holding the value of the watch filter label.
	watchFilterVariable = "WATCH_FILTER"
)

// controlPlaneMachineRecorder records the name of every control plane Machine that ever existed in a namespace,
// including the ones that are deleted afterwards.
type controlPlaneMachineRecorder struct {
	lock     sync.Mutex
	machines map[string]sets.Set[string]
}

func (r *controlPlaneMachineRecorder) record(obj any) {
	machine, ok := obj.(*clusterv1.Machine)
	if !ok {
		return
	}

	if _, isControlPlane := machine.Labels[clusterv1.MachineControlPlaneLabel]; !isControlPlane {
		return
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	clusterName := machine.Spec.ClusterName
	if r.machines[clusterName] == nil {
		r.machines[clusterName] = sets.New[string]()
	}

	r.machines[clusterName].Insert(machine.Name)
}

func (r *controlPlaneMachineRecorder) names(clusterName string) []string {
	r.lock.Lock()
	defer r.lock.Unlock()

	return sets.List(r.machines[clusterName])
}

var _ = Describe("Watch filter", Label(DefaultTestsLabel), func() {
	const (
		instanceA = "instance-a"
		instanceB = "instance-b"
		// instanceNone is a watch filter value that no provider instance is configured with.
		instanceNone = "instance-none"
	)

	var (
		specName            = "watch-filter"
		namespace           *corev1.Namespace
		cancelWatches       context.CancelFunc
		resultA             *ApplyClusterTemplateAndWaitResult
		resultB             *ApplyClusterTemplateAndWaitResult
		clusterNameA        string
		clusterNameB        string
		clusterNameNone     string
		clusterctlLogFolder string
	)

	BeforeEach(func() {
		Expect(e2eConfig).ToNot(BeNil(), "Invalid argument. e2eConfig can't be nil when calling %s spec", specName)
		Expect(clusterctlConfigPath).To(BeAnExistingFile(), "Invalid argument. clusterctlConfigPath must be an existing file when calling %s spec", specName)
		Expect(bootstrapClusterProxy).ToNot(BeNil(), "Invalid argument. bootstrapClusterProxy can't be nil when calling %s spec", specName)
		Expect(os.MkdirAll(artifactFolder, 0755)).To(Succeed(), "Invalid argument. artifactFolder can't be created for %s spec", specName)

		Expect(e2eConfig.Variables).To(HaveKey(KubernetesVersion))
		Expect(e2eConfig.Variables).To(HaveKey(KubernetesVersionUpgradeTo))

		By("Initializing the bootstrap cluster")
		initBootstrapCluster(bootstrapClusterProxy, e2eConfig, clusterctlConfigPath, artifactFolder)

		suffix := util.RandomString(6)
		clusterNameA = fmt.Sprintf("caprke2-e2e-%s-a", suffix)
		clusterNameB = fmt.Sprintf("caprke2-e2e-%s-b", suffix)
		clusterNameNone = fmt.Sprintf("caprke2-e2e-%s-none", suffix)

		// Setup a Namespace where to host objects for this spec and create a watcher for the namespace events.
		namespace, cancelWatches = setupSpecNamespace(ctx, specName, bootstrapClusterProxy, artifactFolder)

		resultA = new(ApplyClusterTemplateAndWaitResult)
		resultB = new(ApplyClusterTemplateAndWaitResult)

		// We need to override clusterctl apply log folder to avoid getting our credentials exposed.
		clusterctlLogFolder = filepath.Join(os.TempDir(), "clusters", bootstrapClusterProxy.GetName())
	})

	AfterEach(func() {
		err := CollectArtifacts(ctx, bootstrapClusterProxy.GetKubeconfigPath(), filepath.Join(artifactFolder, bootstrapClusterProxy.GetName(), clusterNameA+specName))
		Expect(err).ToNot(HaveOccurred())

		cleanInput := cleanupInput{
			SpecName:             specName,
			Cluster:              resultA.Cluster,
			KubeconfigPath:       resultA.KubeconfigPath,
			ClusterProxy:         bootstrapClusterProxy,
			Namespace:            namespace,
			CancelWatches:        cancelWatches,
			IntervalsGetter:      e2eConfig.GetIntervals,
			SkipCleanup:          skipCleanup,
			ArtifactFolder:       artifactFolder,
			AdditionalCleanup:    cleanupInstallation(ctx, clusterctlLogFolder, clusterctlConfigPath, bootstrapClusterProxy),
			ClusterctlConfigPath: clusterctlConfigPath,
		}

		dumpSpecResourcesAndCleanup(ctx, cleanInput)

		if !skipCleanup && resultB.KubeconfigPath != "" {
			Expect(os.Remove(resultB.KubeconfigPath)).To(Succeed())
		}
	})

	It("Should only reconcile the clusters matching the watch filter of each provider instance", func() {
		mgmtClient := bootstrapClusterProxy.GetClient()

		// withWatchFilter returns the args of a manager with the watch filter set to the given value.
		withWatchFilter := func(args []string, value string) []string {
			args = slices.DeleteFunc(slices.Clone(args), func(arg string) bool {
				return strings.HasPrefix(arg, watchFilterFlag)
			})

			return append(args, watchFilterFlag+value)
		}

		By("Running two instances of the bootstrap and control plane providers with different watch filters")
		// clusterctl labels the components of a provider with the manifest label of the provider name in the e2e config.
		providers := []string{
			clusterctlv1.ManifestLabel("rke2-bootstrap", clusterctlv1.BootstrapProviderType),
			clusterctlv1.ManifestLabel("rke2-control-plane", clusterctlv1.ControlPlaneProviderType),
		}
		for _, provider := range providers {
			deployments := &appsv1.DeploymentList{}
			Expect(mgmtClient.List(ctx, deployments, client.MatchingLabels{clusterv1.ProviderNameLabel: provider})).To(Succeed())
			Expect(deployments.Items).To(HaveLen(1), "Expected a single Deployment for the %s provider", provider)

			deployment := &deployments.Items[0]
			Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(1))
			// The Pods of both instances match the selector of the original Deployment: clusterctl relabels the
			// Deployment but not its Pod template, and the second instance only adds a label to it.
			podSelector := client.MatchingLabels(maps.Clone(deployment.Spec.Selector.MatchLabels))

			// The second instance is a copy of the Deployment installed by clusterctl. It keeps the labels of the
			// original one, so that it is selected by the webhook Service and removed together with the provider.
			second := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deployment.Name + "-" + instanceB,
					Namespace: deployment.Namespace,
					Labels:    deployment.Labels,
				},
				Spec: *deployment.Spec.DeepCopy(),
			}
			second.Spec.Selector.MatchLabels[watchFilterInstanceLabel] = instanceB
			second.Spec.Template.Labels[watchFilterInstanceLabel] = instanceB
			second.Spec.Template.Spec.Containers[0].Args = withWatchFilter(second.Spec.Template.Spec.Containers[0].Args, instanceB)
			Expect(mgmtClient.Create(ctx, second)).To(Succeed())

			originalArgs := slices.Clone(deployment.Spec.Template.Spec.Containers[0].Args)
			patch := client.MergeFrom(deployment.DeepCopy())
			deployment.Spec.Template.Spec.Containers[0].Args = withWatchFilter(originalArgs, instanceA)
			Expect(mgmtClient.Patch(ctx, deployment, patch)).To(Succeed())

			// Restore the provider as installed by clusterctl whatever happens to the rest of the spec, so that a
			// failure here cannot leave the following specs with filtered instances. The providers may already be
			// gone when this runs, if the installation was cleaned up first.
			DeferCleanup(func() {
				if skipCleanup {
					return
				}

				Expect(client.IgnoreNotFound(mgmtClient.Delete(ctx, second))).To(Succeed())

				current := &appsv1.Deployment{}
				err := mgmtClient.Get(ctx, client.ObjectKeyFromObject(deployment), current)
				if apierrors.IsNotFound(err) {
					return
				}
				Expect(err).ToNot(HaveOccurred())

				patch := client.MergeFrom(current.DeepCopy())
				current.Spec.Template.Spec.Containers[0].Args = originalArgs
				Expect(mgmtClient.Patch(ctx, current, patch)).To(Succeed())
			})

			// Wait until the only Pods left are one per instance, both of them ready and running with a watch filter.
			// This guarantees that no Pod without a watch filter is running when the clusters are created.
			Eventually(func(g Gomega) {
				pods := &corev1.PodList{}
				g.Expect(mgmtClient.List(ctx, pods, client.InNamespace(deployment.Namespace), podSelector)).To(Succeed())

				filters := []string{}
				for _, pod := range pods.Items {
					g.Expect(pod.Spec.Containers).To(HaveLen(1))
					g.Expect(pod.Status.ContainerStatuses).To(HaveLen(1))
					g.Expect(pod.Status.ContainerStatuses[0].Ready).To(BeTrue(), "Pod %s is not ready", pod.Name)

					for _, arg := range pod.Spec.Containers[0].Args {
						if strings.HasPrefix(arg, watchFilterFlag) {
							filters = append(filters, strings.TrimPrefix(arg, watchFilterFlag))
						}
					}
				}

				g.Expect(pods.Items).To(HaveLen(2))
				g.Expect(filters).To(ConsistOf(instanceA, instanceB))
			}, e2eConfig.GetIntervals(specName, "wait-controllers")...).Should(Succeed())
		}

		By("Recording the control plane Machines created in the namespace")
		recorder := &controlPlaneMachineRecorder{machines: map[string]sets.Set[string]{}}
		machineCache, err := cache.New(bootstrapClusterProxy.GetRESTConfig(), cache.Options{
			Scheme:            bootstrapClusterProxy.GetScheme(),
			DefaultNamespaces: map[string]cache.Config{namespace.Name: {}},
		})
		Expect(err).ToNot(HaveOccurred())

		machineInformer, err := machineCache.GetInformer(ctx, &clusterv1.Machine{})
		Expect(err).ToNot(HaveOccurred())
		_, err = machineInformer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{AddFunc: recorder.record})
		Expect(err).ToNot(HaveOccurred())

		cacheCtx, cancelCache := context.WithCancel(ctx)
		defer cancelCache()

		go func() {
			defer GinkgoRecover()
			Expect(machineCache.Start(cacheCtx)).To(Succeed())
		}()
		Expect(machineCache.WaitForCacheSync(ctx)).To(BeTrue())

		// expectControlPlaneMachines checks the number of control plane Machines that ever existed for a cluster.
		// The recorder never forgets a Machine, so any Machine created in excess makes this fail.
		expectControlPlaneMachines := func(clusterName string, count int) {
			Eventually(func() []string {
				return recorder.names(clusterName)
			}, "1m", "1s").Should(HaveLen(count), "Unexpected control plane Machines for the cluster %s", clusterName)
		}

		configCluster := func(clusterName, watchFilter, kubernetesVersion string, controlPlaneMachineCount int64) clusterctl.ConfigClusterInput {
			return clusterctl.ConfigClusterInput{
				LogFolder:                clusterctlLogFolder,
				ClusterctlConfigPath:     clusterctlConfigPath,
				KubeconfigPath:           bootstrapClusterProxy.GetKubeconfigPath(),
				InfrastructureProvider:   "docker",
				Flavor:                   "in-memory-watch-filter",
				Namespace:                namespace.Name,
				ClusterName:              clusterName,
				KubernetesVersion:        kubernetesVersion,
				ControlPlaneMachineCount: ptr.To(controlPlaneMachineCount),
				WorkerMachineCount:       ptr.To(int64(1)),
				ClusterctlVariables:      map[string]string{watchFilterVariable: watchFilter},
			}
		}

		applyClusterTemplateAndWait := func(input clusterctl.ConfigClusterInput, result *ApplyClusterTemplateAndWaitResult) {
			ApplyClusterTemplateAndWait(ctx, ApplyClusterTemplateAndWaitInput{
				ClusterProxy:                 bootstrapClusterProxy,
				ConfigCluster:                input,
				WaitForClusterIntervals:      e2eConfig.GetIntervals(specName, "wait-cluster"),
				WaitForControlPlaneIntervals: e2eConfig.GetIntervals(specName, "wait-control-plane"),
				WaitForMachineDeployments:    e2eConfig.GetIntervals(specName, "wait-worker-nodes"),
				// In-memory machines run no RKE2, and CAPRKE2 waits for this secret to mark the control plane initialized.
				PreWaitForCluster: func() {
					kubeconfigKey := client.ObjectKey{Namespace: namespace.Name, Name: input.ClusterName + "-kubeconfig"}
					Eventually(func() error {
						return mgmtClient.Get(ctx, kubeconfigKey, &corev1.Secret{})
					}, e2eConfig.GetIntervals(specName, "wait-control-plane")...).Should(Succeed())

					workloadClient := bootstrapClusterProxy.GetWorkloadCluster(ctx, namespace.Name, input.ClusterName).GetClient()
					Eventually(func() error {
						return client.IgnoreAlreadyExists(workloadClient.Create(ctx, &corev1.Secret{
							ObjectMeta: metav1.ObjectMeta{Name: "rke2-serving", Namespace: metav1.NamespaceSystem},
						}))
					}, e2eConfig.GetIntervals(specName, "wait-control-plane")...).Should(Succeed())
				},
			}, result)
		}

		By("Creating a cluster that does not match the watch filter of any provider instance")
		noneTemplate := clusterctl.ConfigCluster(ctx, configCluster(clusterNameNone, instanceNone, e2eConfig.MustGetVariable(KubernetesVersion), 1))
		Expect(noneTemplate).ToNot(BeNil(), "Failed to get the cluster template")
		Eventually(func() error {
			return Apply(ctx, bootstrapClusterProxy, noneTemplate)
		}, e2eConfig.GetIntervals(specName, "wait-cluster")...).Should(Succeed(), "Failed to apply the cluster template")

		By("Creating a cluster with 3 control plane machines matching the watch filter of the first instance")
		applyClusterTemplateAndWait(configCluster(clusterNameA, instanceA, e2eConfig.MustGetVariable(KubernetesVersion), 3), resultA)

		By("Creating a cluster matching the watch filter of the second instance")
		applyClusterTemplateAndWait(configCluster(clusterNameB, instanceB, e2eConfig.MustGetVariable(KubernetesVersion), 1), resultB)

		By("Checking that a single Machine was created for every control plane replica")
		expectControlPlaneMachines(clusterNameA, 3)
		expectControlPlaneMachines(clusterNameB, 1)

		By("Rolling out the control plane of the first cluster while both instances are running")
		versionAfterUpgrade := e2eConfig.MustGetVariable(KubernetesVersionUpgradeTo)
		applyClusterTemplateAndWait(configCluster(clusterNameA, instanceA, versionAfterUpgrade, 3), resultA)

		Eventually(func(g Gomega) {
			machines := &clusterv1.MachineList{}
			g.Expect(mgmtClient.List(ctx, machines, client.InNamespace(namespace.Name), client.MatchingLabels{
				clusterv1.ClusterNameLabel:         clusterNameA,
				clusterv1.MachineControlPlaneLabel: "",
			})).To(Succeed())
			g.Expect(machines.Items).To(HaveLen(3))

			for _, machine := range machines.Items {
				g.Expect(machine.Spec.Version).To(Equal(versionAfterUpgrade+"+rke2r1"), "Machine %s is not upgraded", machine.Name)
				g.Expect(machine.DeletionTimestamp.IsZero()).To(BeTrue(), "Machine %s is being deleted", machine.Name)
			}

			rcp := &controlplanev1.RKE2ControlPlane{}
			g.Expect(mgmtClient.Get(ctx, client.ObjectKeyFromObject(resultA.ControlPlane), rcp)).To(Succeed())
			g.Expect(rcp.Status.Replicas).To(HaveValue(BeEquivalentTo(3)))
			g.Expect(rcp.Status.ReadyReplicas).To(HaveValue(BeEquivalentTo(3)))
			g.Expect(rcp.Status.UpToDateReplicas).To(HaveValue(BeEquivalentTo(3)))
		}, e2eConfig.GetIntervals(specName, "wait-control-plane")...).Should(Succeed())

		By("Checking that the rollout created a single replacement for every control plane Machine")
		expectControlPlaneMachines(clusterNameA, 6)
		expectControlPlaneMachines(clusterNameB, 1)

		// The other clusters were provisioned and rolled out in the meantime,
		// which gave the providers plenty of time to reconcile this one, if they were not ignoring it.
		By("Checking that the cluster not matching any watch filter was ignored by both instances")
		noneControlPlane := &controlplanev1.RKE2ControlPlane{}
		Expect(mgmtClient.Get(ctx, client.ObjectKey{Namespace: namespace.Name, Name: clusterNameNone + "-control-plane"}, noneControlPlane)).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(noneControlPlane, controlplanev1.RKE2ControlPlaneFinalizer)).To(BeFalse(),
			"The RKE2ControlPlane %s was reconciled", noneControlPlane.Name)
		Expect(noneControlPlane.Status.Conditions).To(BeEmpty(), "The RKE2ControlPlane %s was reconciled", noneControlPlane.Name)
		Expect(recorder.names(clusterNameNone)).To(BeEmpty(), "Unexpected control plane Machines for the cluster %s", clusterNameNone)

		noneConfigs := &bootstrapv1.RKE2ConfigList{}
		Expect(mgmtClient.List(ctx, noneConfigs, client.InNamespace(namespace.Name),
			client.MatchingLabels{clusterv1.ClusterNameLabel: clusterNameNone})).To(Succeed())
		// The control plane RKE2Configs are created by the control plane instance that reconciles the cluster, and
		// Cluster API does not create the worker Machines, nor their RKE2Configs, before the control plane is
		// initialized. A cluster ignored by every instance has no RKE2Config at all.
		Expect(noneConfigs.Items).To(BeEmpty(), "Unexpected RKE2Configs for the cluster %s", clusterNameNone)
	})
})
