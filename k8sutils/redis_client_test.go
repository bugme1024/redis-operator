package k8sutils

import (
	redisv1beta1 "redis-operator/api/v1beta1"

	"testing"
)

func TestRedisClient(t *testing.T) {
	var cr *redisv1beta1.RedisSentinel
	cr.Namespace = "test"
	cr.ObjectMeta.Name = "test"
	client := configureRedisClient()
	t.Log(client)
}
