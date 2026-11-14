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
	"maps"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/test/builder"

	bootstrapv1 "github.com/rancher/cluster-api-provider-rke2/bootstrap/api/v1beta2"
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

	// startReconciler runs the RKE2Config controller in a dedicated manager,
	// so that every spec can set up the controller with a different watch filter value.
	startReconciler := func(filterValue string) {
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

		Expect((&RKE2ConfigReconciler{
			Client:           mgr.GetClient(),
			Scheme:           mgr.GetScheme(),
			WatchFilterValue: filterValue,
		}).SetupWithManager(mgrCtx, mgr, 1)).To(Succeed())

		DeferCleanup(func() {
			cancel()
			Eventually(done, 30*time.Second).Should(BeClosed())
		})

		go func() {
			defer GinkgoRecover()
			defer close(done)

			Expect(mgr.Start(mgrCtx)).To(Succeed())
		}()
	}

	// createConfig creates an RKE2Config together with its Cluster and Machine,
	// each of them carrying the given labels, and generates an event for each of them.
	createConfig := func(name string, configLabels, machineLabels, clusterLabels map[string]string) *bootstrapv1.RKE2Config {
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

		ownerLabels := map[string]string{clusterv1.ClusterNameLabel: cluster.Name}
		maps.Copy(ownerLabels, machineLabels)

		machine := &clusterv1.Machine{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    ownerLabels,
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

		rke2Config := &bootstrapv1.RKE2Config{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    configLabels,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine")),
				},
			},
		}
		Expect(testEnv.Create(ctx, rke2Config)).To(Succeed())

		// Generate a Machine and a Cluster event mapping to the RKE2Config.
		machinePatch := client.MergeFrom(machine.DeepCopy())
		machine.Annotations = map[string]string{"watch-filter-test": "true"}
		Expect(testEnv.Patch(ctx, machine, machinePatch)).To(Succeed())

		clusterPatch := client.MergeFrom(cluster.DeepCopy())
		cluster.Annotations = map[string]string{"watch-filter-test": "true"}
		Expect(testEnv.Patch(ctx, cluster, clusterPatch)).To(Succeed())

		return rke2Config
	}

	// createMachinePoolConfig creates an RKE2Config together with its Cluster and MachinePool,
	// and generates a MachinePool event for it. Only the MachinePool carries the given labels.
	createMachinePoolConfig := func(name string, machinePoolLabels map[string]string) *bootstrapv1.RKE2Config {
		cluster := &clusterv1.Cluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
			},
			Spec: clusterv1.ClusterSpec{
				ClusterNetwork: clusterv1.ClusterNetwork{
					Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"192.168.0.0/16"}},
					Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"192.169.0.0/16"}},
				},
			},
		}
		Expect(testEnv.Create(ctx, cluster)).To(Succeed())

		machinePool := &clusterv1.MachinePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				Labels:    machinePoolLabels,
			},
			Spec: clusterv1.MachinePoolSpec{
				ClusterName: cluster.Name,
				Replicas:    ptr.To[int32](1),
				Template: clusterv1.MachineTemplateSpec{
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
							Kind:     builder.GenericInfrastructureMachinePoolKind,
							APIGroup: builder.InfrastructureGroupVersion.Group,
							Name:     name,
						},
					},
				},
			},
		}
		Expect(testEnv.Create(ctx, machinePool)).To(Succeed())

		rke2Config := &bootstrapv1.RKE2Config{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns.Name,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(machinePool, clusterv1.GroupVersion.WithKind("MachinePool")),
				},
			},
		}
		Expect(testEnv.Create(ctx, rke2Config)).To(Succeed())

		// Generate a MachinePool event mapping to the RKE2Config.
		machinePoolPatch := client.MergeFrom(machinePool.DeepCopy())
		machinePool.Annotations = map[string]string{"watch-filter-test": "true"}
		Expect(testEnv.Patch(ctx, machinePool, machinePoolPatch)).To(Succeed())

		return rke2Config
	}

	// isReconciled reports whether the controller reconciled the RKE2Config,
	// which is observed through the conditions set by the first reconciliation.
	isReconciled := func(rke2Config *bootstrapv1.RKE2Config) func(g Gomega) bool {
		return func(g Gomega) bool {
			g.Expect(testEnv.Get(ctx, client.ObjectKeyFromObject(rke2Config), rke2Config)).To(Succeed())

			return conditions.Has(rke2Config, bootstrapv1.RKE2ConfigDataSecretAvailableCondition)
		}
	}

	It("should reconcile every RKE2Config when no watch filter is set", func() {
		startReconciler("")

		unlabeled := createConfig("unlabeled", nil, nil, nil)
		matching := createConfig("matching", matchingLabels, matchingLabels, matchingLabels)

		Eventually(isReconciled(unlabeled), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		Eventually(isReconciled(matching), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("should only reconcile RKE2Configs matching the watch filter", func() {
		startReconciler(watchFilterValue)

		otherLabels := map[string]string{clusterv1.WatchLabel: "instance-b"}

		unlabeled := createConfig("unlabeled", nil, nil, nil)
		other := createConfig("other", otherLabels, otherLabels, otherLabels)
		matching := createConfig("matching", matchingLabels, matchingLabels, matchingLabels)

		Eventually(isReconciled(matching), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		Consistently(isReconciled(unlabeled), 5*time.Second, 250*time.Millisecond).Should(BeFalse())
		Consistently(isReconciled(other), 5*time.Second, 250*time.Millisecond).Should(BeFalse())
	})

	It("should reconcile an RKE2Config on the events of a Machine matching the watch filter", func() {
		startReconciler(watchFilterValue)

		rke2Config := createConfig("matching-machine", nil, matchingLabels, nil)

		Eventually(isReconciled(rke2Config), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("should reconcile an RKE2Config on the events of a Cluster matching the watch filter", func() {
		startReconciler(watchFilterValue)

		rke2Config := createConfig("matching-cluster", nil, nil, matchingLabels)

		Eventually(isReconciled(rke2Config), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
	})

	It("should only reconcile an RKE2Config on the events of a MachinePool matching the watch filter", func() {
		if !feature.Gates.Enabled(feature.MachinePool) {
			Skip("The MachinePool feature gate is not enabled")
		}

		startReconciler(watchFilterValue)

		unlabeled := createMachinePoolConfig("unlabeled-pool", nil)
		matching := createMachinePoolConfig("matching-pool", matchingLabels)

		Eventually(isReconciled(matching), 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		Consistently(isReconciled(unlabeled), 5*time.Second, 250*time.Millisecond).Should(BeFalse())
	})
})
