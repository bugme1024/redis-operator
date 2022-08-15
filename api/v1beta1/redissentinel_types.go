/*
Copyright 2020 Opstree Solutions.

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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type Sentinel struct {
	// +kubebuilder:validation:Minimum=3
	Replicas        *int32                       `json:"replicas,omitempty"`
	Enabled         bool                         `json:"enabled,omitempty"`
	Image           string                       `json:"image"`
	Resources       *corev1.ResourceRequirements `json:"resources,omitempty"`
	ImagePullPolicy corev1.PullPolicy            `json:"imagePullPolicy,omitempty"`
	EnvVars         *[]corev1.EnvVar             `json:"env,omitempty"`
	RedisConfig     *SentinelConfig              `json:"sentinelConfig,omitempty"`
	Affinity        *corev1.Affinity             `json:"affinity,omitempty"`
	ReadinessProbe  *Probe                       `json:"readinessProbe,omitempty" protobuf:"bytes,11,opt,name=readinessProbe"`
	LivenessProbe   *Probe                       `json:"livenessProbe,omitempty" protobuf:"bytes,11,opt,name=livenessProbe"`
}

// RedisClusterStatus defines the observed state of RedisCluster
type RedisSentinelStatus struct {
}

type RedisSentinelSpec struct {
	// +kubebuilder:validation:Minimum=3
	KubernetesConfig KubernetesConfig `json:"kubernetesConfig"`
	// +kubebuilder:default:={livenessProbe:{initialDelaySeconds: 1, timeoutSeconds: 1, periodSeconds: 10, successThreshold: 1, failureThreshold:3}, readinessProbe:{initialDelaySeconds: 1, timeoutSeconds: 1, periodSeconds: 10, successThreshold: 1, failureThreshold:3}}
	Redis RedisLeader `json:"redis,omitempty"`
	// +kubebuilder:default:={livenessProbe:{initialDelaySeconds: 1, timeoutSeconds: 1, periodSeconds: 10, successThreshold: 1, failureThreshold:3}, readinessProbe:{initialDelaySeconds: 1, timeoutSeconds: 1, periodSeconds: 10, successThreshold: 1, failureThreshold:3}}
	Sentinel          Sentinel                     `json:"sentinel,omitempty"`
	RedisExporter     *RedisExporter               `json:"redisExporter,omitempty"`
	Storage           *Storage                     `json:"storage,omitempty"`
	NodeSelector      map[string]string            `json:"nodeSelector,omitempty"`
	SecurityContext   *corev1.PodSecurityContext   `json:"securityContext,omitempty"`
	PriorityClassName string                       `json:"priorityClassName,omitempty"`
	Tolerations       *[]corev1.Toleration         `json:"tolerations,omitempty"`
	Resources         *corev1.ResourceRequirements `json:"resources,omitempty"`
	TLS               *TLSConfig                   `json:"TLS,omitempty"`
	Sidecars          *[]Sidecar                   `json:"sidecars,omitempty"`
}

func (cr *RedisSentinelSpec) GetReplicaCounts(t string) int32 {
	var replica int32
	if t == "redis" && cr.Redis.Replicas != nil {
		replica = *cr.Redis.Replicas
	} else if t == "sentinel" && cr.Sentinel.Replicas != nil {
		replica = *cr.Sentinel.Replicas
	}
	return replica
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ClusterSize",type=integer,JSONPath=`.spec.clusterSize`,description=Current cluster node count
// +kubebuilder:printcolumn:name="LeaderReplicas",type=integer,JSONPath=`.spec.redisLeader.replicas`,description=Overridden Leader replica count
// +kubebuilder:printcolumn:name="FollowerReplicas",type=integer,JSONPath=`.spec.redisFollower.replicas`,description=Overridden Follower replica count
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`,description=Age of Cluster
// RedisCluster is the Schema for the redisclusters API
type RedisSentinel struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisSentinelSpec   `json:"spec"`
	Status RedisSentinelStatus `json:"status,omitempty"`
}

func (cr *RedisSentinel) GetMasterName() string {
	if cr.ObjectMeta.Name == "" {
		return "unknow-name-master"
	}
	return cr.ObjectMeta.Name + "-" + "master"
}

//+kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisSentinelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisSentinel `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisSentinel{}, &RedisSentinelList{})
}
