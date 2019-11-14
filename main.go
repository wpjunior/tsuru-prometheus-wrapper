package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pkg/errors"
)

func getUniqueID() (string, error) {
	hostName, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%d", hostName, os.Getpid()), nil
}

type DeploymentElection struct {
	instanceName   string
	uniqueId       string
	client         *clientv3.Client
	session        *concurrency.Session
	electedReplica int64 // 0: No elected, 1 - Primary, 2 - Secondary
	waitChan       chan bool
}

func NewDeploymentElection(instanceName string, etcdHosts []string, disableSecondary bool) (*DeploymentElection, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdHosts,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Could not connect into etcd servers")
	}

	uniqueId, err := getUniqueID()
	if err != nil {
		return nil, errors.Wrap(err, "Could not get a unique id for this host")
	}

	session, err := concurrency.NewSession(
		cli,
		concurrency.WithTTL(10),
	)

	if err != nil {
		return nil, errors.Wrap(err, "Could not setup a etcd session")
	}

	election := &DeploymentElection{
		instanceName: instanceName,
		uniqueId:     uniqueId,
		client:       cli,
		session:      session,
		waitChan:     make(chan bool, 0),
	}

	go election.waitForARole(1)
	if !disableSecondary {
		go election.waitForARole(2)
	}

	return election, nil
}

func (election *DeploymentElection) waitForARole(replica int64) {
	key := fmt.Sprintf("/prometheus/locks/%s/replicas/%d", election.instanceName, replica)

	etcdElection := concurrency.NewElection(
		election.session,
		key,
	)

	err := etcdElection.Campaign(
		context.Background(),
		election.uniqueId,
	)

	if err != nil {
		fmt.Println("error", err)
		return
	}

	swapped := atomic.CompareAndSwapInt64(&election.electedReplica, 0, replica)
	if swapped {
		election.waitChan <- true
	} else {
		etcdElection.Resign(context.Background())
	}
}

func (election *DeploymentElection) Wait() int64 {
	<-election.waitChan
	return election.electedReplica
}

func (election *DeploymentElection) Close() {
	election.session.Close()
}

func main() {
	var (
		instanceName     = ""
		etcdHosts        = ""
		dataPath         = ""
		disableSecondary = false
	)

	flag.StringVar(&instanceName, "instance_name", "", "")
	flag.StringVar(&etcdHosts, "etcd_hosts", "", "")
	flag.StringVar(&dataPath, "data_path", "", "")
	flag.BoolVar(&disableSecondary, "disable_secondary", false, "")

	flag.Parse()

	if os.Getenv("PROMETHEUS_DISABLE_SECONDARY") == "true" {
		log.Println("Environment variable PROMETHEUS_DISABLE_SECONDARY is true, disabling secondary replica support")
		disableSecondary = true
	}

	hosts := strings.SplitN(etcdHosts, ",", -1)

	election, err := NewDeploymentElection(instanceName, hosts, disableSecondary)
	if err != nil {
		log.Fatal(err)
	}

	defer election.Close()

	log.Println("Wait to become a replica")
	replicaNumber := election.Wait()

	log.Printf("Becoming a replica number %d", replicaNumber)

	targetArgs := getPrometheusArgs()
	targetArgs = append(targetArgs, "--storage.tsdb.path",
		fmt.Sprintf("%s/replica-%d", dataPath, replicaNumber))

	err = runPrometheus(targetArgs)

	if err != nil {
		log.Fatal(err)
	}
}

func runPrometheus(args []string) error {
	cmd := exec.Command("prometheus", args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	return cmd.Run()
}

func getPrometheusArgs() []string {
	index := -1
	for argIndex, arg := range os.Args {
		if arg == "--" {
			index = argIndex + 1
			break
		}
	}

	return os.Args[index:]
}
