package controllers

import (
	"testing"

	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/test/builder"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bootstrapv1 "github.com/rancher/cluster-api-provider-rke2/bootstrap/api/v1beta2"
	controlplanev1 "github.com/rancher/cluster-api-provider-rke2/controlplane/api/v1beta2"
)

func TestComputeDesiredMachineTaints(t *testing.T) {
	taints := []clusterv1.MachineTaint{
		{Key: "dedicated", Value: "control-plane", Effect: corev1.TaintEffectNoSchedule, Propagation: clusterv1.MachineTaintPropagationAlways},
		{Key: "initializing", Effect: corev1.TaintEffectPreferNoSchedule, Propagation: clusterv1.MachineTaintPropagationOnInitialization},
	}
	for _, tt := range []struct {
		name     string
		existing *clusterv1.Machine
		desired  []clusterv1.MachineTaint
	}{
		{name: "new machine", desired: taints},
		{
			name: "existing machine",
			existing: &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Name: "existing", UID: "original"},
				Spec:       clusterv1.MachineSpec{Version: "v1.34.2+rke2r1"},
			},
			desired: taints,
		},
		{
			name: "remove taints",
			existing: &clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{Name: "existing", UID: "original"},
				Spec:       clusterv1.MachineSpec{Version: "v1.34.2+rke2r1", Taints: taints},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			rcp := &controlplanev1.RKE2ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: "control-plane", Namespace: "default"},
				Spec: controlplanev1.RKE2ControlPlaneSpec{
					Version: "v1.35.0+rke2r1",
					MachineTemplate: controlplanev1.RKE2ControlPlaneMachineTemplate{
						Spec: controlplanev1.RKE2ControlPlaneMachineTemplateSpec{Taints: tt.desired},
					},
				},
			}
			cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster"}}
			r := &RKE2ControlPlaneReconciler{}
			machine, err := r.computeDesiredMachine(rcp, cluster,
				clusterv1.ContractVersionedObjectReference{}, clusterv1.ContractVersionedObjectReference{}, "", tt.existing)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(machine.Spec.Taints).To(Equal(tt.desired))
			if tt.existing != nil {
				g.Expect(machine.Name).To(Equal(tt.existing.Name))
				g.Expect(machine.UID).To(Equal(tt.existing.UID))
				g.Expect(machine.Spec.Version).To(Equal(tt.existing.Spec.Version))
			}
		})
	}
}

func TestCloneConfigsAndGenerateMachineLabels(t *testing.T) {
	g := NewWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(clusterv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(bootstrapv1.AddToScheme(scheme)).To(Succeed())
	g.Expect(controlplanev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(apiextensionsv1.AddToScheme(scheme)).To(Succeed())

	infraTemplate := builder.InfrastructureMachineTemplate(metav1.NamespaceDefault, "infra-template").Build()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: metav1.NamespaceDefault}}
	rcp := &controlplanev1.RKE2ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "control-plane", Namespace: metav1.NamespaceDefault, UID: "rcp-uid"},
		Spec: controlplanev1.RKE2ControlPlaneSpec{
			Version: "v1.35.0+rke2r1",
			MachineTemplate: controlplanev1.RKE2ControlPlaneMachineTemplate{
				ObjectMeta: clusterv1.ObjectMeta{
					Labels: map[string]string{clusterv1.WatchLabel: "instance-a"},
				},
				Spec: controlplanev1.RKE2ControlPlaneMachineTemplateSpec{
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{
						Kind:     infraTemplate.GetKind(),
						APIGroup: infraTemplate.GroupVersionKind().Group,
						Name:     infraTemplate.GetName(),
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(builder.GenericInfrastructureMachineTemplateCRD.DeepCopy(), infraTemplate).Build()
	r := &RKE2ControlPlaneReconciler{Client: c}

	g.Expect(r.cloneConfigsAndGenerateMachine(t.Context(), cluster, rcp, &bootstrapv1.RKE2ConfigSpec{}, "")).To(Succeed())

	// The objects created for a control plane Machine must carry the same labels as the Machine,
	// including the ones coming from the machine template, e.g. the watch filter label.
	expectedLabels := ControlPlaneMachineLabelsForCluster(rcp, cluster.Name)
	g.Expect(expectedLabels).To(HaveKeyWithValue(clusterv1.WatchLabel, "instance-a"))

	machines := &clusterv1.MachineList{}
	g.Expect(c.List(t.Context(), machines)).To(Succeed())
	g.Expect(machines.Items).To(HaveLen(1))
	g.Expect(machines.Items[0].Labels).To(Equal(expectedLabels))

	rke2Configs := &bootstrapv1.RKE2ConfigList{}
	g.Expect(c.List(t.Context(), rke2Configs)).To(Succeed())
	g.Expect(rke2Configs.Items).To(HaveLen(1))
	g.Expect(rke2Configs.Items[0].Labels).To(Equal(expectedLabels))

	infraMachines := &unstructured.UnstructuredList{}
	infraMachines.SetGroupVersionKind(builder.InfrastructureGroupVersion.WithKind(builder.GenericInfrastructureMachineKind + "List"))
	g.Expect(c.List(t.Context(), infraMachines)).To(Succeed())
	g.Expect(infraMachines.Items).To(HaveLen(1))
	g.Expect(infraMachines.Items[0].GetLabels()).To(Equal(expectedLabels))
}
