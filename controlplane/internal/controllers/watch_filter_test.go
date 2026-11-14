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

package controllers

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	bootstrapv1 "github.com/rancher/cluster-api-provider-rke2/bootstrap/api/v1beta2"
	controlplanev1 "github.com/rancher/cluster-api-provider-rke2/controlplane/api/v1beta2"
	"github.com/rancher/cluster-api-provider-rke2/pkg/rke2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/test/builder"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var _ = Describe("Watch filter", func() {
	const watchFilterValue = "instance-a"

	matchingLabels := map[string]string{clusterv1.WatchLabel: watchFilterValue}

	var ns *corev1.Namespace

	BeforeEach(func() {
		var err error
		ns, err = testEnv.CreateNamespace(ctx, "watch-filter")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(testEnv.Cleanup(ctx, ns)).To(Succeed())
	})

	// startReconciler runs the RKE2ControlPlane controller in a dedicated manager, so that every spec
	// can set up the controller with a different watch filter value, and returns the ClusterCache
	// created by the controller.
	startReconciler := func(filterValue string) clustercache.ClusterCache {
		mgrCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})

		mgr, err := ctrl.NewManager(testEnv.Config, manager.Options{
			Scheme: testEnv.GetScheme(),
			// Only watch the namespace of the spec, to ignore objects left over by other specs.
			Cache:      cache.Options{DefaultNamespaces: map[string]cache.Config{ns.Name: {}}},
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: config.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).ToNot(HaveOccurred())

		r := &RKE2ControlPlaneReconciler{
			Client:              mgr.GetClient(),
			Scheme:              mgr.GetScheme(),
			SecretCachingClient: mgr.GetClient(),
			WatchFilterValue:    filterValue,
		}
		Expect(r.SetupWithManager(mgrCtx, mgr, 20, 30, 1, 1)).To(Succeed())

		management, ok := r.managementCluster.(*rke2.Management)
		Expect(ok).To(BeTrue())

		DeferCleanup(func() {
			// Drop the connections to the workload clusters, they are not bound to the manager.
			clusterCache, ok := management.ClusterCache.(interface{ Shutdown() })
			Expect(ok).To(BeTrue(), "the ClusterCache no longer exposes Shutdown")
			clusterCache.Shutdown()
			cancel()
			Eventually(done, 30*time.Second).Should(BeClosed())
		})

		go func() {
			defer GinkgoRecover()
			defer close(done)

			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()

		return management.ClusterCache
	}

	// createControlPlane creates an RKE2ControlPlane together with its Cluster and a Machine,
	// each of them carrying the given labels, and generates an event for each of them.
	createControlPlane := func(name string, rcpLabels, clusterLabels, machineLabels map[string]string) *controlplanev1.RKE2ControlPlane {
		cluster := &clusterv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    clusterLabels,
			},
			Spec: clusterv1.ClusterSpec{
				ClusterNetwork: clusterv1.ClusterNetwork{
					Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16"}},
					Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.169.0.0/16"}},
				},
			},
		}
		Expect(testEnv.Create(ctx, cluster)).To(Succeed())

		rcp := &controlplanev1.RKE2ControlPlane{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    rcpLabels,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: clusterv1.GroupVersion.String(),
					Kind:       clusterv1.ClusterKind,
					Name:       cluster.Name,
					UID:        cluster.UID,
				}},
			},
			Spec: controlplanev1.RKE2ControlPlaneSpec{
				Version:         RKE2KubernetesVersion,
				RolloutStrategy: &controlplanev1.RolloutStrategy{Type: controlplanev1.RollingUpdateStrategyType},
				MachineTemplate: controlplanev1.RKE2ControlPlaneMachineTemplate{
					Spec: controlplanev1.RKE2ControlPlaneMachineTemplateSpec{
						InfrastructureRef: clusterv1.ContractVersionedObjectReference{
							Kind:     builder.GenericInfrastructureMachineTemplateKind,
							APIGroup: builder.InfrastructureGroupVersion.Group,
							Name:     name,
						},
					},
				},
			},
		}
		Expect(testEnv.Create(ctx, rcp)).To(Succeed())

		// Generate a Cluster event mapping to the RKE2ControlPlane.
		patch := client.MergeFrom(cluster.DeepCopy())
		cluster.Spec.ControlPlaneRef = clusterv1.ContractVersionedObjectReference{
			Kind:     rke2ControlPlaneKind,
			APIGroup: controlplanev1.GroupVersion.Group,
			Name:     rcp.Name,
		}
		Expect(testEnv.Patch(ctx, cluster, patch)).To(Succeed())

		// Generate a Machine event mapping to the RKE2ControlPlane.
		machine := &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    machineLabels,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(rcp, controlplanev1.GroupVersion.WithKind(rke2ControlPlaneKind)),
				},
			},
			Spec: clusterv1.MachineSpec{
				ClusterName: cluster.Name,
				Bootstrap: clusterv1.Bootstrap{
					ConfigRef: clusterv1.ContractVersionedObjectReference{
						Kind:     "RKE2Config",
						APIGroup: bootstrapv1.GroupVersion.Group,
						Name:     name,
					},
				},
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{
					Kind:     builder.GenericInfrastructureMachineKind,
					APIGroup: builder.InfrastructureGroupVersion.Group,
					Name:     name,
				},
			},
		}
		Expect(testEnv.Create(ctx, machine)).To(Succeed())

		return rcp
	}

	// createConnectableCluster creates a Cluster that the ClusterCache is able to connect to,
	// using the test environment as the workload cluster.
	createConnectableCluster := func(name string, labels map[string]string) client.ObjectKey {
		cluster := &clusterv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    labels,
			},
			Spec: clusterv1.ClusterSpec{
				ClusterNetwork: clusterv1.ClusterNetwork{
					Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16"}},
					Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.169.0.0/16"}},
				},
			},
		}
		Expect(testEnv.Create(ctx, cluster)).To(Succeed())

		Expect(testEnv.Create(ctx, kubeconfig.GenerateSecretWithOwner(
			client.ObjectKeyFromObject(cluster),
			kubeconfig.FromEnvTestConfig(testEnv.Config, cluster),
			metav1.OwnerReference{
				APIVersion: clusterv1.GroupVersion.String(),
				Kind:       clusterv1.ClusterKind,
				Name:       cluster.Name,
				UID:        cluster.UID,
			}))).To(Succeed())

		// The ClusterCache only connects once the infrastructure is provisioned.
		patch := client.MergeFrom(cluster.DeepCopy())
		cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
		Expect(testEnv.Status().Patch(ctx, cluster, patch)).To(Succeed())

		return client.ObjectKeyFromObject(cluster)
	}

	// isReconciled reports whether the controller reconciled the RKE2ControlPlane,
	// which is observed through the finalizer added by the first reconciliation.
	isReconciled := func(rcp *controlplanev1.RKE2ControlPlane) func(g Gomega) bool {
		return func(g Gomega) bool {
			g.Expect(testEnv.Get(ctx, client.ObjectKeyFromObject(rcp), rcp)).To(Succeed())

			return controllerutil.ContainsFinalizer(rcp, controlplanev1.RKE2ControlPlaneFinalizer)
		}
	}

	// isConnected reports whether the ClusterCache has a connection to the given Cluster.
	isConnected := func(clusterCache clustercache.ClusterCache, cluster client.ObjectKey) func() error {
		return func() error {
			_, err := clusterCache.GetClient(ctx, cluster)

			return err
		}
	}

	It("should reconcile every RKE2ControlPlane when no watch filter is set", func() {
		clusterCache := startReconciler("")

		unlabeled := createControlPlane("unlabeled", nil, nil, nil)
		matching := createControlPlane("matching", matchingLabels, matchingLabels, matchingLabels)

		Eventually(isReconciled(unlabeled), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		Eventually(isReconciled(matching), 30*time.Second, 250*time.Millisecond).Should(BeTrue())

		unlabeledCluster := createConnectableCluster("unlabeled-cluster", nil)
		matchingCluster := createConnectableCluster("matching-cluster", matchingLabels)

		Eventually(isConnected(clusterCache, unlabeledCluster), 30*time.Second, 250*time.Millisecond).Should(Succeed())
		Eventually(isConnected(clusterCache, matchingCluster), 30*time.Second, 250*time.Millisecond).Should(Succeed())
	})

	It("should only reconcile RKE2ControlPlanes matching the watch filter", func() {
		startReconciler(watchFilterValue)

		otherLabels := map[string]string{clusterv1.WatchLabel: "instance-b"}

		unlabeled := createControlPlane("unlabeled", nil, nil, nil)
		other := createControlPlane("other", otherLabels, otherLabels, otherLabels)
		matching := createControlPlane("matching", matchingLabels, matchingLabels, matchingLabels)

		Eventually(isReconciled(matching), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		Consistently(isReconciled(unlabeled), 5*time.Second, 250*time.Millisecond).Should(BeFalse())
		Consistently(isReconciled(other), 5*time.Second, 250*time.Millisecond).Should(BeFalse())
	})

	It("should reconcile an RKE2ControlPlane on the events of a Cluster matching the watch filter", func() {
		startReconciler(watchFilterValue)

		rcp := createControlPlane("matching-cluster", nil, matchingLabels, nil)

		Eventually(isReconciled(rcp), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("should reconcile an RKE2ControlPlane on the events of a Machine matching the watch filter", func() {
		startReconciler(watchFilterValue)

		rcp := createControlPlane("matching-machine", nil, nil, matchingLabels)

		Eventually(isReconciled(rcp), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("should only connect to the workload clusters matching the watch filter", func() {
		clusterCache := startReconciler(watchFilterValue)

		unlabeled := createConnectableCluster("unlabeled", nil)
		other := createConnectableCluster("other", map[string]string{clusterv1.WatchLabel: "instance-b"})
		matching := createConnectableCluster("matching", matchingLabels)

		Eventually(isConnected(clusterCache, matching), 30*time.Second, 250*time.Millisecond).Should(Succeed())
		Consistently(isConnected(clusterCache, unlabeled), 5*time.Second, 250*time.Millisecond).
			Should(MatchError(clustercache.ErrClusterNotConnected))
		Consistently(isConnected(clusterCache, other), 5*time.Second, 250*time.Millisecond).
			Should(MatchError(clustercache.ErrClusterNotConnected))
	})
})
