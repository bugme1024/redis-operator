package k8sutils

import (
	redisv1beta1 "redis-operator/api/v1beta1"

	corev1 "k8s.io/api/core/v1"
)

type RedisSentinelSTS struct {
	RedisClusterSTS
}

// generateRedisSentinelParams generates Redis sentinel information
func generateRedisSentinelParams(cr *redisv1beta1.RedisSentinel, replicas int32, externalConfig *string, affinity *corev1.Affinity) statefulSetParameters {
	res := statefulSetParameters{
		Metadata:          cr.ObjectMeta,
		Replicas:          &replicas,
		NodeSelector:      cr.Spec.NodeSelector,
		SecurityContext:   cr.Spec.SecurityContext,
		PriorityClassName: cr.Spec.PriorityClassName,
		Affinity:          affinity,
		Tolerations:       cr.Spec.Tolerations,
	}
	if cr.Spec.RedisExporter != nil {
		res.EnableMetrics = cr.Spec.RedisExporter.Enabled
	}
	if cr.Spec.KubernetesConfig.ImagePullSecrets != nil {
		res.ImagePullSecrets = cr.Spec.KubernetesConfig.ImagePullSecrets
	}
	if cr.Spec.Storage != nil {
		res.PersistentVolumeClaim = cr.Spec.Storage.VolumeClaimTemplate
	}
	if externalConfig != nil {
		res.ExternalConfig = externalConfig
	}
	return res
}

// generateRedisSentinelContainerParams generates Redis container information
func generateRedisSentinelContainerParams(cr *redisv1beta1.RedisSentinel, readinessProbeDef *redisv1beta1.Probe, livenessProbeDef *redisv1beta1.Probe) containerParameters {
	trueProperty := true
	falseProperty := false
	containerProp := containerParameters{
		Role:            "cluster",
		Image:           cr.Spec.KubernetesConfig.Image,
		ImagePullPolicy: cr.Spec.KubernetesConfig.ImagePullPolicy,
		Resources:       cr.Spec.KubernetesConfig.Resources,
	}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		containerProp.EnabledPassword = &trueProperty
		containerProp.SecretName = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name
		containerProp.SecretKey = cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key
	} else {
		containerProp.EnabledPassword = &falseProperty
	}
	if cr.Spec.RedisExporter != nil {
		containerProp.RedisExporterImage = cr.Spec.RedisExporter.Image
		containerProp.RedisExporterImagePullPolicy = cr.Spec.RedisExporter.ImagePullPolicy

		if cr.Spec.RedisExporter.Resources != nil {
			containerProp.RedisExporterResources = cr.Spec.RedisExporter.Resources
		}

		if cr.Spec.RedisExporter.EnvVars != nil {
			containerProp.RedisExporterEnv = cr.Spec.RedisExporter.EnvVars
		}

	}
	if readinessProbeDef != nil {
		containerProp.ReadinessProbe = readinessProbeDef
	}
	if livenessProbeDef != nil {
		containerProp.LivenessProbe = livenessProbeDef
	}
	if cr.Spec.Storage != nil {
		containerProp.PersistenceEnabled = &trueProperty
	}
	if cr.Spec.TLS != nil {
		containerProp.TLSConfig = cr.Spec.TLS
	}

	return containerProp
}

// CreateRedisFollower will create a follower redis setup
func CreateRedis(cr *redisv1beta1.RedisSentinel) error {
	prop := RedisSentinelSTS{
		RedisClusterSTS: RedisClusterSTS{
			RedisStateFulType: "redis",
			Affinity:          cr.Spec.Redis.Affinity,
			ReadinessProbe:    cr.Spec.Redis.ReadinessProbe,
			LivenessProbe:     cr.Spec.Redis.LivenessProbe,
		},
	}
	if cr.Spec.RedisFollower.RedisConfig != nil {
		prop.ExternalConfig = cr.Spec.Redis.RedisConfig.AdditionalRedisConfig
	}
	return prop.CreateRedisClusterSetup(cr)
}

// CreateRedisFollower will create a follower redis setup
func CreateSentinel(cr *redisv1beta1.RedisSentinel) error {
	prop := RedisSentinelSTS{
		RedisStateFulType: "sentinel",
		Affinity:          cr.Spec.Sentinel.Affinity,
		ReadinessProbe:    cr.Spec.Sentinel.ReadinessProbe,
		LivenessProbe:     cr.Spec.Sentinel.LivenessProbe,
	}
	if cr.Spec.RedisFollower.RedisConfig != nil {
		prop.ExternalConfig = cr.Spec.Sentinel.RedisConfig.AdditionalRedisConfig
	}
	return prop.CreateRedisClusterSetup(cr)
}

// CreateRedisClusterSetup will create Redis Setup for leader and follower
func (service RedisSentinelSTS) CreateRedisSentinelSetup(cr *redisv1beta1.RedisSentinel) error {
	stateFulName := cr.ObjectMeta.Name + "-" + service.RedisStateFulType
	logger := statefulSetLogger(cr.Namespace, stateFulName)
	labels := getRedisLabels(stateFulName, "redissentinel", service.RedisStateFulType, cr.ObjectMeta.Labels)
	annotations := generateStatefulSetsAnots(cr.ObjectMeta)
	objectMetaInfo := generateObjectMetaInformation(stateFulName, cr.Namespace, labels, annotations)
	err := CreateOrUpdateStateFul(
		cr.Namespace,
		objectMetaInfo,
		generateRedisClusterParams(cr, service.getReplicaCount(cr), service.ExternalConfig, service.Affinity),
		redisClusterAsOwner(cr),
		generateRedisClusterContainerParams(cr, service.ReadinessProbe, service.LivenessProbe),
		cr.Spec.Sidecars,
	)
	if err != nil {
		logger.Error(err, "Cannot create statefulset for Redis", "Setup.Type", service.RedisStateFulType)
		return err
	}
	return nil
}

// CreateRedisSentinelService method will create service for Redis
func (service RedisSentinelSTS) CreateRedisSentinelService(cr *redisv1beta1.RedisSentinel) error {
	serviceName := cr.ObjectMeta.Name + "-" + service.RedisServiceRole
	logger := serviceLogger(cr.Namespace, serviceName)
	labels := getRedisLabels(serviceName, "redissentinel", service.RedisServiceRole, cr.ObjectMeta.Labels)
	annotations := generateServiceAnots(cr.ObjectMeta)
	if cr.Spec.RedisExporter != nil && cr.Spec.RedisExporter.Enabled {
		enableMetrics = true
	}
	objectMetaInfo := generateObjectMetaInformation(serviceName, cr.Namespace, labels, annotations)
	headlessObjectMetaInfo := generateObjectMetaInformation(serviceName+"-headless", cr.Namespace, labels, annotations)
	err := CreateOrUpdateService(cr.Namespace, headlessObjectMetaInfo, redisSentinelAsOwner(cr), false, true)
	if err != nil {
		logger.Error(err, "Cannot create headless service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	err = CreateOrUpdateService(cr.Namespace, objectMetaInfo, redisSentinelAsOwner(cr), enableMetrics, false)
	if err != nil {
		logger.Error(err, "Cannot create service for Redis", "Setup.Type", service.RedisServiceRole)
		return err
	}
	return nil
}
