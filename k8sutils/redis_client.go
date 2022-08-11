package k8sutils

import (
	"context"
	"errors"
	"fmt"
	redisv1beta1 "redis-operator/api/v1beta1"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/go-redis/redis"
)

const (
	redisPort       = "6379"
	sentinelPort    = "26379"
	RedisPodType    = "redis"
	SentinelPodType = "sentinel"
	redisRoleMaster = "role:master"

	redisSyncing           = "master_sync_in_progress:1"
	redisMasterSillPending = "master_host:127.0.0.1"
	redisLinkUp            = "master_link_status:up"

	sentinelsNumberREString = "sentinels=([0-9]+)"
	slaveNumberREString     = "slaves=([0-9]+)"
	sentinelStatusREString  = "status=([a-z]+)"
	redisMasterHostREString = "master_host:([0-9.]+)"
)

var (
	sentinelNumberRE  = regexp.MustCompile(sentinelsNumberREString)
	sentinelStatusRE  = regexp.MustCompile(sentinelStatusREString)
	slaveNumberRE     = regexp.MustCompile(slaveNumberREString)
	redisMasterHostRE = regexp.MustCompile(redisMasterHostREString)
)

// func getPassWithSecret(cr *redisv1beta1.RedisSentinel) (string, error) {
// 	var pass string
// 	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
// 		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
// 		return "", err
// 	} else {
// 		pass = ""
// 	}
// 	return pass, nil
// }

// configureRedisClient will configure the Redis Client
func configureRedisSentinelClient(cr *redisv1beta1.RedisSentinel, podName string, podType string) *redis.Client {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)
	redisInfo := RedisDetails{
		PodName:   podName,
		Namespace: cr.Namespace,
	}
	var client *redis.Client
	var portStr string
	var pass string
	switch podType {
	case RedisPodType:
		portStr = redisPort
	case SentinelPodType:
		portStr = sentinelPort
	default:
		portStr = redisPort
	}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		pass, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
	} else {
		pass = ""
	}

	// connect sentinel: no need set password
	if podType == "" {
		pass = ""
	}

	client = redis.NewClient(&redis.Options{
		Addr:      getRedisServerIP(redisInfo) + portStr,
		Password:  pass,
		DB:        0,
		TLSConfig: getRedisSentinelTLSConfig(cr, redisInfo),
	})
	return client
}

func isSentinelReady(info string) error {
	matchStatus := sentinelStatusRE.FindStringSubmatch(info)
	if len(matchStatus) == 0 || matchStatus[1] != "ok" {
		return errors.New("Sentinels not ready")
	}
	return nil
}

func getQuorum(rf *redisv1beta1.RedisSentinel) int32 {
	return rf.Spec.Sentinel.Replicas/2 + 1
}

// GetStatefulSetPods will give a list of pods that are managed by the statefulset
func GetMasterPods(cr *redisv1beta1.RedisSentinel) ([]string, error) {
	namespace := cr.ObjectMeta.Namespace
	name := cr.ObjectMeta.Name
	client := generateK8sClient()
	statefulSet, err := GetStatefulSet(namespace, name+"-redis")
	var podNames []string
	if err != nil {
		return []string{}, err
	}
	labels := []string{}
	for k, v := range statefulSet.Spec.Selector.MatchLabels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}
	selector := strings.Join(labels, ",")
	podList := client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for _, sp := range podList.Items {
		if sp.Status.Phase == corev1.PodRunning && sp.DeletionTimestamp == nil { // Only work with running pods
			podNames = append(podNames, sp.Status.PodIP)
		}
	}
	return podNames, nil
}
func GetNumberOfSentinels(cr *redisv1beta1.RedisSentinel, podName string) (int32, error) {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	info, err := client.Info("sentinel").Result()
	if err != nil {
		return 0, err
	}
	if err2 := isSentinelReady(info); err2 != nil {
		return 0, err2
	}
	match := sentinelNumberRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return 0, errors.New("Seninel regex not found")
	}
	nSentinels, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(nSentinels), nil
}

func GetSlavesCountFromSentinel(cr *redisv1beta1.RedisCluster, podName string) (int32, error) {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	info, err := client.Info("sentinel").Result()
	if err != nil {
		return 0, err
	}
	if err2 := isSentinelReady(info); err2 != nil {
		return 0, err2
	}
	match := slaveNumberRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return 0, errors.New("Slaves regex not found")
	}
	nSlaves, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}
	return int32(nSlaves), nil

}

func ResetSentinel(cr *redisv1beta1.RedisCluster, podName string) error {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	defer client.Close()
	cmd := redis.NewIntCmd("SENTINEL", "reset", "*")
	err := client.Process(cmd)
	if err != nil {
		return err
	}
	_, err = cmd.Result()
	if err != nil {
		return err
	}
	return nil
}

// GetSlaveOf returns the master of the given redis, or nil if it's master
func GetSlaveOf(cr *redisv1beta1.RedisCluster, podName string) (string, error) {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)

	defer client.Close()
	info, err := client.Info("replication").Result()
	if err != nil {
		return "", err
	}
	match := redisMasterHostRE.FindStringSubmatch(info)
	if len(match) == 0 {
		return "", nil
	}
	return match[1], nil
}

// IsMaster returns true if pod is master, or return false if not
func IsMaster(cr *redisv1beta1.RedisCluster, podName string) (bool, error) {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)

	defer client.Close()
	info, err := client.Info("replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Contains(info, redisRoleMaster), nil
}

// MonitorRedisWithPort
func MonitorRedisWithPort(cr *redisv1beta1.RedisCluster, sentinelPodName string, masterPodName string) error {
	logger := generateRedisManagerLogger(cr.Namespace, cr.ObjectMeta.Name)

	var password string
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	masterName := cr.GetMasterName()
	masterPodDetail := RedisDetails{
		PodName:   masterPodName,
		Namespace: cr.ObjectMeta.Namespace,
	}
	masterIp := getRedisServerIP(masterPodDetail)
	cmd := redis.NewBoolCmd("SENTINEL", "REMOVE", masterName)
	_ = client.Process(cmd)
	// We'll continue even if it fails, the priority is to have the redises monitored
	cmd = redis.NewBoolCmd("SENTINEL", "MONITOR", masterName, masterIp, redisPort, getQuorum(cr))
	err := client.Process(cmd)
	if err != nil {
		return err
	}
	_, err = cmd.Result()
	if err != nil {
		return err
	}
	if cr.Spec.KubernetesConfig.ExistingPasswordSecret != nil {
		password, err := getRedisPassword(cr.Namespace, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Name, *cr.Spec.KubernetesConfig.ExistingPasswordSecret.Key)
		if err != nil {
			logger.Error(err, "Error in getting redis password")
		}
	} else {
		password = ""
	}

	if password != "" {
		cmd = client.NewBoolCmd("SENTINEL", "SET", masterName, "auth-pass", password)
		err := client.Process(cmd)
		if err != nil {
			return err
		}
		_, err = cmd.Result()
		if err != nil {
			return err
		}
	}
	return nil
}

func MakeMaster(cr *redisv1beta1.RedisCluster, podName string) error {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	if res := client.SlaveOf("NO", "ONE"); res.Err() != nil {
		return res.Err()
	}
	return nil
}

func MakeSlaveOfWithPort(cr *redisv1beta1.RedisCluster, podName string, masterPodName string) error {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()
	masterPodDetail := RedisDetails{
		PodName:   masterPodName,
		Namespace: cr.ObjectMeta.Namespace,
	}
	masterIp := getRedisServerIP(masterPodDetail)
	if res := client.SlaveOf(masterIp, redisPort); res.Err() != nil {
		return res.Err()
	}
	return nil
}

func GetSentinelMonitor(cr *redisv1beta1.RedisCluster, podName string) (string, string, error) {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()

	cmd := redis.NewSliceCmd("SENTINEL", "master", cr.GetMasterNames())
	err := client.Process(cmd)
	if err != nil {
		return "", "", err
	}
	res, err := cmd.Result()
	if err != nil {
		return "", "", err
	}
	masterIP := res[3].(string)
	masterPort := res[5].(string)
	return masterIP, masterPort, nil
}

func getConfigParameters(config string) (parameter string, value string, err error) {
	s := strings.Split(config, " ")
	if len(s) < 2 {
		return "", "", fmt.Errorf("configuration '%s' malformed", config)
	}
	return s[0], strings.Join(s[1:], " "), nil
}
func SetCustomSentinelConfig(cr *redisv1beta1.RedisCluster, podName string, configs []string) error {
	client := configureRedisSentinelClient(cr, podName, SentinelPodType)
	defer client.Close()

	for _, config := range configs {
		param, value, err := getConfigParameters(config)
		if err != nil {
			return err
		}
		if err := applySentinelConfig(param, value, client); err != nil {
			return err
		}
	}
	return nil
}

func SetCustomRedisConfig(cr *redisv1beta1.RedisCluster, podName string, configs []string, password string) error {
	client := configureRedisSentinelClient(cr, podName, RedisPodType)
	defer client.Close()

	for _, config := range configs {
		param, value, err := getConfigParameters(config)
		if err != nil {
			return err
		}
		if err := applyRedisConfig(param, value, client); err != nil {
			return err
		}
	}
	return nil
}

func applyRedisConfig(parameter string, value string, rClient *redis.Client) error {
	result := rClient.ConfigSet(parameter, value)
	return result.Err()
}

func applySentinelConfig(parameter string, value string, masterName string, rClient *redis.Client) error {
	cmd := redis.NewStatusCmd("SENTINEL", "set", masterName, parameter, value)
	err := rClient.Process(cmd)
	if err != nil {
		return err
	}
	return cmd.Err()
}

func SlaveIsReady(cr *redisv1beta1.RedisCluster, podName string) (bool, error) {
	client := configureRedisSentinelClient(cr, podName, RedisPodType)
	defer client.Close()
	info, err := client.Info("replication").Result()
	if err != nil {
		return false, err
	}

	ok := !strings.Contains(info, redisSyncing) &&
		!strings.Contains(info, redisMasterSillPending) &&
		strings.Contains(info, redisLinkUp)

	return ok, nil
}
