// Copyright 2020 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"os/exec"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/crypto/ssh"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	batchinformers "k8s.io/client-go/informers/batch/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	restclientset "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"

	"github.com/kubeflow/mpi-operator/cmd/mpi-operator/app/options"
	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/validation"
	clientset "github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned"
	"github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kubeflow/mpi-operator/pkg/client/informers/externalversions/kubeflow/v2beta1"
	listers "github.com/kubeflow/mpi-operator/pkg/client/listers/kubeflow/v2beta1"
)

const (
	controllerAgentName     = "mpi-job-controller"
	configSuffix            = "-config"
	configVolumeName        = "mpi-job-config"
	configMountPath         = "/etc/mpi"
	hostfileName            = "hostfile"
	discoverHostsScriptName = "discover_hosts.sh"
	sshAuthSecretSuffix     = "-ssh"
	sshAuthVolume           = "ssh-auth"
	rootSSHPath             = "/root/.ssh"
	launcher                = "launcher"
	worker                  = "worker"
	launcherSuffix          = "-launcher"
	workerSuffix            = "-worker"
	labelGroupName          = "group-name"
	labelMPIJobName         = "mpi-job-name"
	labelMPIRoleType        = "mpi-job-role"
	sshPublicKey            = "ssh-publickey"
	sshPrivateKeyFile       = "id_rsa"
	sshPublicKeyFile        = sshPrivateKeyFile + ".pub"
	sshAuthorizedKeysFile   = "authorized_keys"
)

const (
	expand              = "expand"
	shrink              = "shrink"
	create              = "create"
	complete            = "complete"
	checkQueueFlag      = "checkQueue"
	assignFreeSlotsFlag = "assignFreeSlots"
	noop                = "noop"
	ccsPort             = 1234
)

const (
	created   = "created"
	queued    = "queued"
	running   = "running"
	expanding = "expanding"
	shrinking = "shrinking"
	completed = "completed"
)

const (
	// ErrResourceExists is used as part of the Event 'reason' when an MPIJob
	// fails to sync due to dependent resources of the same name already
	// existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to dependent resources already existing.
	MessageResourceExists = "Resource %q of Kind %q already exists and is not managed by MPIJob"

	// ValidationError is used as part of the Event 'reason' when failed to
	// validate an MPIJob.
	ValidationError = "ValidationError"

	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SetPodTemplateRestartPolicy"

	// eventMessageLimit is the maximum size of an Event's message.
	// From: k8s.io/kubernetes/pkg/apis/core/validation/events.go
	eventMessageLimit = 1024

	// jobBackoffLimitExceededReason is the reason that the k8s job controller
	// uses when the backoff limit is exceeded.
	jobBackoffLimitExceededReason = "BackoffLimitExceeded"

	openMPISlotsEnv  = "OMPI_MCA_orte_set_default_slots"
	intelMPISlotsEnv = "I_MPI_PERHOST"
)

var (
	mpiJobsCreatedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_created_total",
		Help: "Counts number of MPI jobs created",
	})
	mpiJobsSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_successful_total",
		Help: "Counts number of MPI jobs successful",
	})
	mpiJobsFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_failed_total",
		Help: "Counts number of MPI jobs failed",
	})
	mpiJobInfoGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mpi_operator_job_info",
		Help: "Information about MPIJob",
	}, []string{"launcher", "namespace"})

	sshVolumeItems = []corev1.KeyToPath{
		{
			Key:  corev1.SSHAuthPrivateKey,
			Path: sshPrivateKeyFile,
		},
		{
			Key:  sshPublicKey,
			Path: sshPublicKeyFile,
		},
		{
			Key:  sshPublicKey,
			Path: sshAuthorizedKeysFile,
		},
	}
	configVolumeItems = []corev1.KeyToPath{
		{
			Key:  hostfileName,
			Path: hostfileName,
			Mode: ptr.To[int32](0444),
		},
		{
			Key:  discoverHostsScriptName,
			Path: discoverHostsScriptName,
			Mode: ptr.To[int32](0555),
		},
	}

	launcherEnvVars = []corev1.EnvVar{
		{
			Name:  "K_MPI_JOB_ROLE",
			Value: launcher,
		},
	}
	workerEnvVars = []corev1.EnvVar{
		{
			Name:  "K_MPI_JOB_ROLE",
			Value: worker,
		},
	}
	ompiEnvVars = []corev1.EnvVar{
		// Allows driver to reach workers through the Service.
		{
			Name:  "OMPI_MCA_orte_keep_fqdn_hostnames",
			Value: "true",
		},
		{
			Name:  "OMPI_MCA_orte_default_hostfile",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "OMPI_MCA_plm_rsh_args",
			Value: "-o ConnectionAttempts=10",
		},
	}
	intelEnvVars = []corev1.EnvVar{
		{
			Name:  "I_MPI_HYDRA_HOST_FILE",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS",
			Value: "-o ConnectionAttempts=10",
		},
	}
	mpichEnvVars = []corev1.EnvVar{
		{
			Name:  "HYDRA_HOST_FILE",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "HYDRA_LAUNCH_EXTRA_ARGS",
			Value: "-o ConnectionAttempts=10",
		},
	}
	nvidiaDisableEnvVars = []corev1.EnvVar{
		{Name: "NVIDIA_VISIBLE_DEVICES"},
		{Name: "NVIDIA_DRIVER_CAPABILITIES"},
	}

	jobQueuedError = fmt.Errorf("Job queued")
)

// An Item is something we manage in a priority queue.
type Item struct {
	mpiJob    kubeflow.MPIJob // The value of the item; arbitrary.
	priority  int             // The priority of the item in the queue.
	timestamp time.Time       // The time at which the item was created.
	// The index is needed by update and is maintained by the heap.Interface methods.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (it *Item) DeepCopy() Item {
	itNew := new(Item)
	itNew.mpiJob = *it.mpiJob.DeepCopy()
	itNew.priority = it.priority
	itNew.timestamp = it.timestamp
	return *itNew
}

func (pq PriorityQueue) DeepCopy() PriorityQueue {
	cpy := make(PriorityQueue, len(pq))
	for idx, item := range pq {
		*cpy[idx] = item.DeepCopy()
	}
	return cpy
}

func (pq PriorityQueue) Len() int { return len(pq) }

func compare(a *Item, b *Item) int {
	if a.priority == b.priority {
		if a.timestamp.Before(b.timestamp) {
			return -1 // earlier timestamp has higher priority
		} else if a.timestamp.After(b.timestamp) {
			return 1 // earlier timestamp has higher priority
		} else {
			return 0 // equal timestamps
		} // earlier timestamp has higher priority
	}
	return cmp.Compare(b.priority, a.priority)
}

func (pq *PriorityQueue) Push(x any) {
	item := x.(*Item)
	*pq = append(*pq, item)
	slices.SortFunc(*pq, compare)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, priority int) {
	item.priority = priority
	slices.SortFunc(*pq, compare)
	//heap.Fix(pq, item.index)
}

// MPIJobController is the controller implementation for MPIJob resources.
type MPIJobController struct {
	config restclientset.Config
	// kubeClient is a standard kubernetes clientset.
	kubeClient    kubernetes.Interface
	metricsClient metrics.Interface
	// kubeflowClient is a clientset for our own API group.
	kubeflowClient clientset.Interface
	// PodGroupCtrl is a client for PodGroups (volcano and scheduler-plugins).
	PodGroupCtrl PodGroupControl

	configMapLister     corelisters.ConfigMapLister
	configMapSynced     cache.InformerSynced
	secretLister        corelisters.SecretLister
	secretSynced        cache.InformerSynced
	serviceLister       corelisters.ServiceLister
	serviceSynced       cache.InformerSynced
	jobLister           batchlisters.JobLister
	jobSynced           cache.InformerSynced
	podLister           corelisters.PodLister
	podSynced           cache.InformerSynced
	podGroupSynced      cache.InformerSynced
	priorityClassLister schedulinglisters.PriorityClassLister
	priorityClassSynced cache.InformerSynced
	mpiJobLister        listers.MPIJobLister
	mpiJobSynced        cache.InformerSynced

	// queue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	queue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	// To allow injection of updateStatus for testing.
	updateStatusHandler func(mpijob *kubeflow.MPIJob) error

	// Clock for internal use of unit-testing
	clock clock.WithTicker

	latestReplicas    map[string]int32
	configMaps        map[string]string
	jobStatus         map[string]string
	lastAction        map[string]time.Time // last action time
	deferredAction    map[string]string
	oldExpandReplicas map[string]int32

	runningJobs PriorityQueue
	queuedJobs  PriorityQueue

	freeSlots  int
	rescaleGap time.Duration
}

// NewMPIJobController returns a new MPIJob controller.
func NewMPIJobController(
	config *restclientset.Config,
	kubeClient kubernetes.Interface,
	metricsClient metrics.Interface,
	kubeflowClient clientset.Interface,
	volcanoClient volcanoclient.Interface,
	schedClient schedclientset.Interface,
	configMapInformer coreinformers.ConfigMapInformer,
	secretInformer coreinformers.SecretInformer,
	serviceInformer coreinformers.ServiceInformer,
	jobInformer batchinformers.JobInformer,
	podInformer coreinformers.PodInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,
	mpiJobInformer informers.MPIJobInformer,
	namespace, gangSchedulingName string) (*MPIJobController, error) {
	return NewMPIJobControllerWithClock(config, kubeClient, metricsClient, kubeflowClient, volcanoClient, schedClient,
		configMapInformer, secretInformer, serviceInformer, jobInformer, podInformer,
		priorityClassInformer, mpiJobInformer, &clock.RealClock{}, namespace, gangSchedulingName)
}

// NewMPIJobControllerWithClock returns a new MPIJob controller.
func NewMPIJobControllerWithClock(
	config *restclientset.Config,
	kubeClient kubernetes.Interface,
	metricsClient metrics.Interface,
	kubeflowClient clientset.Interface,
	volcanoClient volcanoclient.Interface,
	schedClient schedclientset.Interface,
	configMapInformer coreinformers.ConfigMapInformer,
	secretInformer coreinformers.SecretInformer,
	serviceInformer coreinformers.ServiceInformer,
	jobInformer batchinformers.JobInformer,
	podInformer coreinformers.PodInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,
	mpiJobInformer informers.MPIJobInformer,
	clock clock.WithTicker,
	namespace, gangSchedulingName string) (*MPIJobController, error) {

	// Create event broadcaster.
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	// For the gang scheduling.
	var (
		podGroupCtrl        PodGroupControl
		podGroupSynced      cache.InformerSynced
		priorityClassLister schedulinglisters.PriorityClassLister
		priorityClassSynced cache.InformerSynced
	)
	priorityClassLister = priorityClassInformer.Lister()
	priorityClassSynced = priorityClassInformer.Informer().HasSynced
	if gangSchedulingName == options.GangSchedulerVolcano {
		podGroupCtrl = NewVolcanoCtrl(volcanoClient, namespace, priorityClassLister)
	} else if len(gangSchedulingName) != 0 {
		// Use scheduler-plugins as a default gang-scheduler.
		podGroupCtrl = NewSchedulerPluginsCtrl(schedClient, namespace, gangSchedulingName, priorityClassLister)
	}
	if podGroupCtrl != nil {
		podGroupSynced = podGroupCtrl.PodGroupSharedIndexInformer().HasSynced
	}

	pqRunning := make(PriorityQueue, 0)
	pqQueued := make(PriorityQueue, 0)

	controller := &MPIJobController{
		config:              *config,
		kubeClient:          kubeClient,
		metricsClient:       metricsClient,
		kubeflowClient:      kubeflowClient,
		PodGroupCtrl:        podGroupCtrl,
		configMapLister:     configMapInformer.Lister(),
		configMapSynced:     configMapInformer.Informer().HasSynced,
		secretLister:        secretInformer.Lister(),
		secretSynced:        secretInformer.Informer().HasSynced,
		serviceLister:       serviceInformer.Lister(),
		serviceSynced:       serviceInformer.Informer().HasSynced,
		jobLister:           jobInformer.Lister(),
		jobSynced:           jobInformer.Informer().HasSynced,
		podLister:           podInformer.Lister(),
		podSynced:           podInformer.Informer().HasSynced,
		podGroupSynced:      podGroupSynced,
		priorityClassLister: priorityClassLister,
		priorityClassSynced: priorityClassSynced,
		mpiJobLister:        mpiJobInformer.Lister(),
		mpiJobSynced:        mpiJobInformer.Informer().HasSynced,
		queue:               workqueue.NewRateLimitingQueueWithConfig(workqueue.DefaultControllerRateLimiter(), workqueue.RateLimitingQueueConfig{Name: "MPIJobs"}),
		recorder:            recorder,
		clock:               clock,
		latestReplicas:      make(map[string]int32),
		configMaps:          make(map[string]string),
		jobStatus:           make(map[string]string),
		lastAction:          make(map[string]time.Time),
		deferredAction:      make(map[string]string),
		oldExpandReplicas:   make(map[string]int32),
		runningJobs:         pqRunning,
		queuedJobs:          pqQueued,
		freeSlots:           60,
		rescaleGap:          1 * time.Second, // 3 minutes
	}
	// FIXME fix the free slots!

	controller.updateStatusHandler = controller.doUpdateJobStatus

	// Set up error handlers for informers
	klog.Info("Setting up informer error handlers")
	informers := map[string]cache.SharedInformer{
		"configMapInformer":     configMapInformer.Informer(),
		"secretInformer":        secretInformer.Informer(),
		"serviceInformer":       serviceInformer.Informer(),
		"jobInformer":           jobInformer.Informer(),
		"podInformer":           podInformer.Informer(),
		"priorityClassInformer": priorityClassInformer.Informer(),
		"mpiJobInformer":        mpiJobInformer.Informer(),
	}

	for name, informer := range informers {
		err := informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
			// Pipe to default handler first, which just logs the error
			cache.DefaultWatchErrorHandler(r, err)

			if errors.IsUnauthorized(err) || errors.IsForbidden(err) {
				klog.Fatalf("Unable to sync cache for informer %s: %s. Requesting controller to exit.", name, err)
			}
		})

		if err != nil {
			// return NewMPIJobControllerWithClock(...) (nil, error)
			return nil, fmt.Errorf("unable to set error handler for informer %s: %s", name, err)
		}
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when MPIJob resources change.
	if _, err := mpiJobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.addMPIJob,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueMPIJob(new)
		},
	}); err != nil {
		return nil, err
	}

	// Set up an event handler for when dependent resources change. This
	// handler will lookup the owner of the given resource, and if it is
	// owned by an MPIJob resource will enqueue that MPIJob resource for
	// processing. This way, we don't need to implement custom logic for
	// handling dependent resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	if _, err := configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	}); err != nil {
		return nil, err
	}
	if _, err := secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	}); err != nil {
		return nil, err
	}
	if _, err := serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	}); err != nil {
		return nil, err
	}
	if _, err := jobInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	}); err != nil {
		return nil, err
	}
	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	}); err != nil {
		return nil, err
	}
	if podGroupCtrl != nil {
		if _, err := podGroupCtrl.PodGroupSharedIndexInformer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.handleObject,
			UpdateFunc: controller.handleObjectUpdate,
			DeleteFunc: controller.handleObject,
		}); err != nil {
			return nil, err
		}
		if _, err := priorityClassInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.handleObject,
			UpdateFunc: controller.handleObjectUpdate,
			DeleteFunc: controller.handleObject,
		}); err != nil {
			return nil, err
		}
	}
	return controller, nil
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the work queue and wait for
// workers to finish processing their current work items.
func (c *MPIJobController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches.
	klog.Info("Starting MPIJob controller")

	// Wait for the caches to be synced before starting workers.
	klog.Info("Waiting for informer caches to sync")
	synced := []cache.InformerSynced{
		c.configMapSynced,
		c.secretSynced,
		c.serviceSynced,
		c.jobSynced,
		c.podSynced,
		c.mpiJobSynced,
	}
	if c.PodGroupCtrl != nil {
		synced = append(synced, c.podGroupSynced, c.priorityClassSynced)
	}
	if ok := cache.WaitForCacheSync(stopCh, synced...); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch workers to process MPIJob resources.
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// work queue.
func (c *MPIJobController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the work queue and
// attempt to process it, by calling the syncHandler.
func (c *MPIJobController) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.queue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the work queue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the work queue and attempted again after a back-off
		// period.
		defer c.queue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the work queue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// work queue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// work queue.
		if key, ok = obj.(string); !ok {
			// As the item in the work queue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.queue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// MPIJob resource to be synced.
		if err := c.syncHandler(key); err != nil {
			c.queue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.queue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

func signalRescale(ipAddr string, port int32, oldProcs int32, newProcs int32) error {
	klog.Infof("Running command: %s %s %s %s %s", "./opt/rescale_client", ipAddr, fmt.Sprint(port), fmt.Sprint(oldProcs), fmt.Sprint(newProcs))
	out, err := exec.Command("./opt/rescale_client", ipAddr, fmt.Sprint(port), fmt.Sprint(oldProcs), fmt.Sprint(newProcs)).CombinedOutput()
	if string(out) == "0" {
		klog.Infof("Error when rescaling")
		return fmt.Errorf("Error: %s", string(out))
	}
	klog.Infof("Rescale signal output: %s", string(out))
	if err != nil {
		klog.Infof("Error when rescaling")
		return err
	}
	return nil
}

func (c *MPIJobController) sendRescaleSignal(mpiJob *kubeflow.MPIJob, oldPodCount int32, newPodCount int32) error {
	launcher, err := c.getLauncherJob(mpiJob)
	if err != nil || launcher == nil {
		return err
	}
	launcherPods, err := c.jobPods(launcher)
	if err != nil || launcherPods == nil || len(launcherPods) == 0 {
		return err
	}
	ipAddr := launcherPods[0].Status.PodIP
	return signalRescale(ipAddr, ccsPort, oldPodCount+1, newPodCount+1)
}

func getJobKey(mpiJob *kubeflow.MPIJob) string {
	return mpiJob.Namespace + "/" + mpiJob.Name
}

func (c *MPIJobController) printJobStatuses() {
	klog.Infof("Free slots = %d, %s", c.freeSlots, fmt.Sprint(c.jobStatus))

	qPrios := make([]int, 0)
	for _, item := range c.queuedJobs {
		qPrios = append(qPrios, item.priority)
	}

	//klog.Infof("Running jobs = %s", fmt.Sprint(c.runningJobs))
	klog.Infof("Queued job prios = %s", fmt.Sprint(qPrios))
}

func (c *MPIJobController) writeHostFile(pod *v1.Pod, hostFileString string) (string, string, error) {
	buf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	echoCmd := fmt.Sprintf("echo '%s'", hostFileString)
	command := fmt.Sprintf("%s > %s", echoCmd, configMountPath+"/"+hostfileName)
	klog.Infof("Running command - %s", command)
	request := c.kubeClient.CoreV1().RESTClient().
		Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Command: []string{"/bin/sh", "-c", command},
			Stdin:   false,
			Stdout:  true,
			Stderr:  true,
			TTY:     true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(&c.config, "POST", request.URL())
	err = exec.StreamWithContext(context.TODO(), remotecommand.StreamOptions{
		Stdout: buf,
		Stderr: errBuf,
	})
	if err != nil {
		return "", "", fmt.Errorf("%w Failed executing command %s on %v/%v", err, command, pod.Namespace, pod.Name)
	}
	return buf.String(), errBuf.String(), nil
}

func (c *MPIJobController) assignFreeSlots() error {
	klog.Infof("Freed workers, total free count = %d, running = %d, queued = %d",
		c.freeSlots, len(c.runningJobs), len(c.queuedJobs))
	idxRunning := 0
	idxQueued := 0
	var it, itQueued, itRunning *Item
	var runPriority, queuePriority int32
	var newReplicas int32
	//var action string
	var minTime time.Duration
	minTime = math.MaxInt64
	var launcherCount int32

	for c.freeSlots > 0 && (idxRunning < len(c.runningJobs) || idxQueued < len(c.queuedJobs)) {
		if idxRunning < len(c.runningJobs) {
			itRunning = c.runningJobs[idxRunning]
			runPriority = *itRunning.mpiJob.Spec.Priority
		} else {
			runPriority = -1
		}

		if idxQueued < len(c.queuedJobs) {
			itQueued = c.queuedJobs[idxQueued]
			queuePriority = *itQueued.mpiJob.Spec.Priority
		} else {
			queuePriority = -1
		}

		if runPriority < queuePriority {
			it = itQueued
			c.queuedJobs = append(c.queuedJobs[:idxQueued], c.queuedJobs[idxQueued+1:]...)
			launcherCount = 1
			//action = create
		} else {
			idxRunning += 1
			//action = expand
			it = itRunning
			launcherCount = 0
		}

		workerPodList, err := c.getRunningWorkerPods(&it.mpiJob)
		if err != nil {
			return err
		}
		jobMaxReplicas := *it.mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].MaxReplicas
		jobMinReplicas := *it.mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].MinReplicas
		if int32(len(workerPodList)) < jobMaxReplicas {
			newReplicas = int32(math.Min(float64(jobMaxReplicas),
				float64(int(c.latestReplicas[getJobKey(&it.mpiJob)])+c.freeSlots-int(launcherCount))))
			klog.Infof("Expanding %s to %d, freecount = %d", getJobKey(&it.mpiJob),
				newReplicas, c.freeSlots)

			if newReplicas < jobMinReplicas {
				if launcherCount == 1 {
					c.queuedJobs.Push(it)
					idxQueued += 1
				}
				continue
			}

			if c.jobStatus[getJobKey(&it.mpiJob)] == running && c.clock.Now().Sub(c.lastAction[getJobKey(&it.mpiJob)]) < c.rescaleGap {
				minTime = min(minTime, c.rescaleGap-c.clock.Now().Sub(c.lastAction[getJobKey(&it.mpiJob)]))
				continue
			}

			if c.jobStatus[getJobKey(&it.mpiJob)] == expanding {
				continue
			}

			c.latestReplicas[getJobKey(&it.mpiJob)] = newReplicas
			c.freeSlots -= (int(newReplicas) - len(workerPodList) + int(launcherCount))

			c.oldExpandReplicas[getJobKey(&it.mpiJob)] = int32(len(workerPodList))
			c.queue.AddRateLimited(getJobKey(&it.mpiJob))
			c.lastAction[getJobKey(&it.mpiJob)] = c.clock.Now()
			if c.jobStatus[getJobKey(&it.mpiJob)] == running {
				c.jobStatus[getJobKey(&it.mpiJob)] = expanding
			}
		}
	}
	//c.runningJobs = c.runningJobs[idxRunning:]
	//c.queuedJobs = c.queuedJobs[idxQueued:]

	if minTime < math.MaxInt64 {
		c.queue.AddAfter(assignFreeSlotsFlag, minTime)
	}

	return nil
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the MPIJob resource
// with the current status of the resource.
func (c *MPIJobController) syncHandler(key string) error {
	klog.Infof("syncHandler called for %s", key)
	c.printJobStatuses()
	startTime := c.clock.Now()
	defer func() {
		klog.Infof("Finished syncing job %q (%v)", key, c.clock.Since(startTime))
	}()

	if key == assignFreeSlotsFlag {
		return c.assignFreeSlots()
	}

	if key == checkQueueFlag {
		return c.checkJobQueue()
	}

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the MPIJob with this namespace/name.
	sharedJob, err := c.mpiJobLister.MPIJobs(namespace).Get(name)
	if err != nil {
		// The MPIJob may no longer exist, in which case we stop processing.
		if errors.IsNotFound(err) {
			klog.V(4).Infof("MPIJob has been deleted: %v", key)
			return nil
		}
		return fmt.Errorf("obtaining job: %w", err)
	}

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	mpiJob := sharedJob.DeepCopy()
	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)

	// for mpi job that is terminating, just return.
	if mpiJob.DeletionTimestamp != nil {
		return nil
	}

	if errs := validation.ValidateMPIJob(mpiJob); len(errs) != 0 {
		msg := truncateMessage(fmt.Sprintf("Found validation errors: %v", errs.ToAggregate()))
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ValidationError, msg)
		// Do not requeue
		return nil
	}

	if len(mpiJob.Status.Conditions) == 0 {
		msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
		updateMPIJobConditions(mpiJob, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
		c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobCreated", msg)
		mpiJobsCreatedCount.Inc()
	}

	// CompletionTime is only filled when the launcher Job succeeded or stopped
	// retrying (it reached .spec.backoffLimit). If it's filled, we want to
	// cleanup and stop retrying the MPIJob.
	if isFinished(mpiJob.Status) && mpiJob.Status.CompletionTime != nil {

		if _, ok := c.jobStatus[key]; !ok {
			return nil
		}

		for idx, item := range c.runningJobs {
			if getJobKey(&item.mpiJob) == getJobKey(mpiJob) {
				c.runningJobs = append(c.runningJobs[:idx], c.runningJobs[idx+1:]...)
			}
		}

		if isCleanUpPods(mpiJob.Spec.RunPolicy.CleanPodPolicy) {
			if err := cleanUpWorkerPods(mpiJob, c); err != nil {
				return err
			}

			c.freeSlots += int(c.latestReplicas[key]) + 1 // +1 for the launcher

			// FIXME - assuming tht clean up call is blocking
			// it is possibly async and calls syncHandler after each pod
			// is removed

			// keep expanding jobs in order of priority until you run
			// out of free worker pods
			c.assignFreeSlots()

			delete(c.deferredAction, key)
			delete(c.latestReplicas, key)
			delete(c.lastAction, key)
			delete(c.jobStatus, key)
			delete(c.configMaps, key)

			return c.updateStatusHandler(mpiJob)
		}
		return nil
	}

	// first set StartTime.
	if mpiJob.Status.StartTime == nil && !isMPIJobSuspended(mpiJob) {
		now := metav1.Now()
		mpiJob.Status.StartTime = &now
	}

	// Get the launcher Job for this MPIJob.
	launcher, err := c.getLauncherJob(mpiJob)
	if err != nil {
		return err
	}

	var worker []*corev1.Pod
	var action string
	//var oldPods int32
	// We're done if the launcher either succeeded or failed.
	done := launcher != nil && isJobFinished(launcher)
	if !done {
		_, err := c.getOrCreateService(mpiJob, newJobService(mpiJob))
		if err != nil {
			return fmt.Errorf("getting or creating Service to front workers: %w", err)
		}

		_, err = c.getOrCreateSSHAuthSecret(mpiJob)
		if err != nil {
			return fmt.Errorf("creating SSH auth secret: %w", err)
		}

		if !isMPIJobSuspended(mpiJob) {
			// Get the PodGroup for this MPIJob
			if c.PodGroupCtrl != nil {
				if podGroup, err := c.getOrCreatePodGroups(mpiJob); podGroup == nil || err != nil {
					return err
				}
			}

			lastReplicas, ok := c.latestReplicas[getJobKey(mpiJob)]
			if !ok {
				lastReplicas = 0
			}

			//action, _, newPods, err = c.getAction(mpiJob)

			isExpand := false
			if status, ok := c.jobStatus[getJobKey(mpiJob)]; ok && status == expanding {
				selector, err := workerSelector(mpiJob.Name)
				if err != nil {
					return err
				}
				podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
				if err != nil {
					return err
				}
				if len(podFullList) < int(lastReplicas) {
					isExpand = true // this will be true only during the first pass through synchandler
				}
			}

			if status, ok := c.jobStatus[getJobKey(mpiJob)]; !ok {
				c.latestReplicas[getJobKey(mpiJob)], err = c.calculateWorkerReplicas(mpiJob)
				klog.Infof("Replicas for %s set to %d", getJobKey(mpiJob), c.latestReplicas[getJobKey(mpiJob)])
				if err == jobQueuedError {
					klog.Infof("Queued a job due to low capacity from calculateWorkerReplicas")
					c.enqueueJobInternal(mpiJob)
					return nil
				} else if err != nil {
					return fmt.Errorf("Error: %w", err)
				}
				c.jobStatus[getJobKey(mpiJob)] = created
				c.freeSlots -= int(c.latestReplicas[getJobKey(mpiJob)]) // This is for the workers
				c.freeSlots -= 1                                        // This one is for the launcher
			} else if status == queued && c.latestReplicas[getJobKey(mpiJob)] > 0 {
				c.jobStatus[getJobKey(mpiJob)] = created
			}

			worker, err = c.getOrCreateWorker(mpiJob) // this also deletes workers in case of shrink
			if err == jobQueuedError {
				klog.Infof("Queued a job due to low capacity from getOrCreateWorker")
				c.enqueueJobInternal(mpiJob)
				return nil
			} else if err != nil {
				return err
			}

			config, err := c.getOrCreateConfigMap(mpiJob)
			if config == nil || err != nil {
				return fmt.Errorf("getting or creating ConfigMap: %w", err)
			}
			c.configMaps[getJobKey(mpiJob)] = config.Data[hostfileName]

			if isExpand {
				c.deferredAction[key] = expand
			}

			config, err = c.getConfigMap(mpiJob)
			ready := c.countReadyWorkerPods(worker)
			if ready == len(worker) && err == nil && config.Data[hostfileName] == c.configMaps[getJobKey(mpiJob)] {
				action, ok = c.deferredAction[key]
				if !ok {
					action = noop
				}

				if action == expand {
					klog.Infof("Workers ready to expand %s, last replicas = %s",
						fmt.Sprint(ready), fmt.Sprint(lastReplicas))

					//launcherPods, err := c.jobPods(launcher)

					//stdout, stderr, err := c.writeHostFile(launcherPods[0], config.Data[hostfileName])
					//if err != nil {
					//	fmt.Sprintf("%w", err)
					//	return err
					//}
					//klog.Infof("Out: %s\nErr: %s", stdout, stderr)

					time.Sleep(time.Second * 5) // wait for workers to be ready

					// wait for workers to be ready and send expand signal
					klog.Infof("Sending expand signal to job %q (%v)", key, c.clock.Since(startTime))
					err = c.sendRescaleSignal(mpiJob, c.oldExpandReplicas[getJobKey(mpiJob)], int32(len(worker)))
					if err != nil {
						return err
					}
					c.lastAction[getJobKey(mpiJob)] = c.clock.Now()
					c.jobStatus[getJobKey(mpiJob)] = running
					c.deferredAction[getJobKey(mpiJob)] = noop
				}
			} else {
				klog.Infof("Waiting for workers to be ready %s/%s", fmt.Sprint(ready), fmt.Sprint(len(worker)))
			}
		}

		config, err := c.getConfigMap(mpiJob)
		if launcher == nil {
			if err == nil && config.Data[hostfileName] == c.configMaps[getJobKey(mpiJob)] &&
				c.jobStatus[getJobKey(mpiJob)] == created && c.countReadyWorkerPods(worker) == len(worker) {
				launcher, err = c.kubeClient.BatchV1().Jobs(namespace).Create(context.TODO(), c.newLauncherJob(mpiJob, len(worker)), metav1.CreateOptions{})
				if err != nil {
					c.recorder.Eventf(mpiJob, corev1.EventTypeWarning, mpiJobFailedReason, "launcher pod created failed: %v", err)
					return fmt.Errorf("creating launcher Pod: %w", err)
				}
				c.jobStatus[getJobKey(mpiJob)] = running
				c.lastAction[getJobKey(mpiJob)] = c.clock.Now()
				c.runningJobs.Push(&Item{*mpiJob, int(*mpiJob.Spec.Priority), c.clock.Now()})
			} else {
				klog.V(4).Infof("Waiting for workers %s/%s to start.", mpiJob.Namespace, mpiJob.Name)
			}
		} else {
			launcherPods, err := c.jobPods(launcher)
			if err != nil {
				return err
			}
			if len(launcherPods) > 0 && isPodRunning(launcherPods[0]) {
				klog.Infof("Setting job status for %s to running", getJobKey(mpiJob))
				//c.jobStatus[getJobKey(mpiJob)] = running
				c.checkJobQueue()
			}
		}
	}

	if launcher != nil {
		if isMPIJobSuspended(mpiJob) != isJobSuspended(launcher) {
			// align the suspension state of launcher with the MPIJob
			launcher.Spec.Suspend = ptr.To(isMPIJobSuspended(mpiJob))
			if _, err := c.kubeClient.BatchV1().Jobs(namespace).Update(context.TODO(), launcher, metav1.UpdateOptions{}); err != nil {
				return err
			}
		} else {
			// this is an expand/shrink action
		}
	}

	// cleanup the running worker pods if the MPI job is suspended
	if isMPIJobSuspended(mpiJob) {
		if err := cleanUpWorkerPods(mpiJob, c); err != nil {
			return err
		}
	}

	// Finally, we update the status block of the MPIJob resource to reflect the
	// current state of the world.
	err = c.updateMPIJobStatus(mpiJob, launcher, worker)
	if err != nil {
		return err
	}

	return nil
}

func cleanUpWorkerPods(mpiJob *kubeflow.MPIJob, c *MPIJobController) error {
	if err := c.deleteWorkerPods(mpiJob); err != nil {
		return err
	}
	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeWorker)
	if c.PodGroupCtrl != nil {
		if err := c.deletePodGroups(mpiJob); err != nil {
			return err
		}
	}
	mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Active = 0
	return nil
}

// getLauncherJob gets the launcher Job controlled by this MPIJob.
func (c *MPIJobController) getLauncherJob(mpiJob *kubeflow.MPIJob) (*batchv1.Job, error) {
	launcher, err := c.jobLister.Jobs(mpiJob.Namespace).Get(mpiJob.Name + launcherSuffix)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return nil, err
	}

	// If the launcher is not controlled by this MPIJob resource, we should log
	// a warning to the event recorder and return.
	if !metav1.IsControlledBy(launcher, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, launcher.Name, launcher.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return launcher, fmt.Errorf(msg)
	}

	return launcher, nil
}

// getOrCreatePodGroups will create a PodGroup for gang scheduling by volcano.
func (c *MPIJobController) getOrCreatePodGroups(mpiJob *kubeflow.MPIJob) (metav1.Object, error) {
	newPodGroup := c.PodGroupCtrl.newPodGroup(mpiJob)
	podGroup, err := c.PodGroupCtrl.getPodGroup(newPodGroup.GetNamespace(), newPodGroup.GetName())
	// If the PodGroup doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		return c.PodGroupCtrl.createPodGroup(context.TODO(), newPodGroup)
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}
	// If the PodGroup is not controlled by this MPIJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(podGroup, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, podGroup.GetName(), "PodGroup")
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	if !c.PodGroupCtrl.pgSpecsAreEqual(podGroup, newPodGroup) {
		return c.PodGroupCtrl.updatePodGroup(context.TODO(), podGroup, newPodGroup)
	}
	return podGroup, nil
}

// deletePodGroups will delete a PodGroup when MPIJob have done.
func (c *MPIJobController) deletePodGroups(mpiJob *kubeflow.MPIJob) error {
	podGroup, err := c.PodGroupCtrl.getPodGroup(mpiJob.Namespace, mpiJob.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// If the PodGroup is not controlled by this MPIJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(podGroup, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, podGroup.GetName(), "PodGroup")
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf(msg)
	}

	// If the PodGroup exist, we'll delete it.
	err = c.PodGroupCtrl.deletePodGroup(context.TODO(), mpiJob.Namespace, mpiJob.Name)
	// If an error occurs during Delete, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	return nil
}

// getRunningWorkerPods get all worker Pods with Running phase controlled by this MPIJob.
func (c *MPIJobController) getRunningWorkerPods(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	selector, err := workerSelector(mpiJob.Name)
	if err != nil {
		return nil, err
	}
	podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	// Only running Pods should be included within the `discover_hosts.sh` script.
	var podList []*corev1.Pod
	for idx, pod := range podFullList {
		if pod.Status.Phase == corev1.PodRunning {
			podList = append(podList, podFullList[idx])
		}
	}

	return podList, nil
}

func (c *MPIJobController) countReadyWorkerPods(workers []*corev1.Pod) int {
	ready := 0
	for _, pod := range workers {
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready++
				break
			}
		}
	}
	return ready
}

func (c *MPIJobController) getConfigMap(mpiJob *kubeflow.MPIJob) (*corev1.ConfigMap, error) {
	cm, err := c.configMapLister.ConfigMaps(mpiJob.Namespace).Get(mpiJob.Name + configSuffix)
	if err != nil {
		return nil, err
	}
	return cm, nil
}

// getOrCreateConfigMap gets the ConfigMap controlled by this MPIJob, or creates
// one if it doesn't exist.
func (c *MPIJobController) getOrCreateConfigMap(mpiJob *kubeflow.MPIJob) (*corev1.ConfigMap, error) {
	klog.Infof("create config called for %s", getJobKey(mpiJob))
	podList, err := c.getRunningWorkerPods(mpiJob)
	if err != nil {
		return nil, err
	}
	newCM := newConfigMap(mpiJob, c.workerReplicas(mpiJob), podList)
	updateDiscoverHostsInConfigMap(newCM, mpiJob, podList)

	cm, err := c.configMapLister.ConfigMaps(mpiJob.Namespace).Get(mpiJob.Name + configSuffix)
	// If the ConfigMap doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		return c.kubeClient.CoreV1().ConfigMaps(mpiJob.Namespace).Create(context.TODO(), newCM, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}

	// If the ConfigMap is not controlled by this MPIJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(cm, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, cm.Name, cm.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the ConfigMap is changed, update it
	if !equality.Semantic.DeepEqual(cm.Data, newCM.Data) {
		klog.Infof("Update config map for job %s: %s", getJobKey(mpiJob), fmt.Sprint(newCM.Data))
		cm = cm.DeepCopy()
		cm.Data = newCM.Data
		cm, err = c.kubeClient.CoreV1().ConfigMaps(mpiJob.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		cmCheck, _ := c.configMapLister.ConfigMaps(mpiJob.Namespace).Get(mpiJob.Name + configSuffix)
		klog.Infof("After CM update: %s", fmt.Sprint(cmCheck.Data))
	}

	return cm, nil
}

func (c *MPIJobController) getOrCreateService(job *kubeflow.MPIJob, newSvc *corev1.Service) (*corev1.Service, error) {
	svc, err := c.serviceLister.Services(job.Namespace).Get(newSvc.Name)
	if errors.IsNotFound(err) {
		return c.kubeClient.CoreV1().Services(job.Namespace).Create(context.TODO(), newSvc, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(svc, job) {
		msg := fmt.Sprintf(MessageResourceExists, svc.Name, svc.Kind)
		c.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the Service selector is changed, update it.
	if !equality.Semantic.DeepEqual(svc.Spec.Selector, newSvc.Spec.Selector) {
		svc = svc.DeepCopy()
		svc.Spec.Selector = newSvc.Spec.Selector
		return c.kubeClient.CoreV1().Services(svc.Namespace).Update(context.TODO(), svc, metav1.UpdateOptions{})
	}

	return svc, nil
}

// getOrCreateSSHAuthSecret gets the Secret holding the SSH auth for this job,
// or create one if it doesn't exist.
func (c *MPIJobController) getOrCreateSSHAuthSecret(job *kubeflow.MPIJob) (*corev1.Secret, error) {
	secret, err := c.secretLister.Secrets(job.Namespace).Get(job.Name + sshAuthSecretSuffix)
	if errors.IsNotFound(err) {
		secret, err := newSSHAuthSecret(job)
		if err != nil {
			return nil, err
		}
		return c.kubeClient.CoreV1().Secrets(job.Namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(secret, job) {
		msg := fmt.Sprintf(MessageResourceExists, secret.Name, secret.Kind)
		c.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}
	newSecret, err := newSSHAuthSecret(job)
	if err != nil {
		return nil, fmt.Errorf("generating new secret: %w", err)
	}
	hasKeys := keysFromData(secret.Data)
	wantKeys := keysFromData(newSecret.Data)
	if !equality.Semantic.DeepEqual(hasKeys, wantKeys) {
		secret := secret.DeepCopy()
		secret.Data = newSecret.Data
		return c.kubeClient.CoreV1().Secrets(secret.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	}
	return secret, nil
}

func keysFromData(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

/*func (c *MPIJobController) getAction(mpiJob *kubeflow.MPIJob) (string, int32, int32, error) {
	if status, ok := c.jobStatus[getJobKey(mpiJob)]; !ok {
		return create, 0, 0, nil
	} else if status == running {
		worker := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
		if worker == nil {
			return noop, 0, 0, nil
		}

		// Remove Pods when replicas are scaled down
		// TODO: shrink the job before scaling down the pods
		selector, err := workerSelector(mpiJob.Name)
		if err != nil {
			return noop, 0, 0, err
		}
		podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
		if err != nil {
			return noop, 0, 0, err
		}

		if len(podFullList) > int(*worker.MaxReplicas) {
			return shrink, int32(len(podFullList)), *worker.MaxReplicas, nil
		} else if len(podFullList) < int(*worker.MaxReplicas) {
			//newWorkerCount := *worker.MaxReplicas
			replicas := *worker.MaxReplicas
			//if newWorkerCount > c.clusterAvailSize {
			//	replicas = int32(math.Min(float64(c.clusterAvailSize), float64(newWorkerCount)))
			//}
			return expand, int32(len(podFullList)), replicas, nil
		} else {
			return noop, 0, 0, nil
		}
	}
}*/

// Adds a new worker to mpiJob
func (c *MPIJobController) addWorker(mpiJob *kubeflow.MPIJob, workerIndex int) (*corev1.Pod, error) {
	pod, err := c.podLister.Pods(mpiJob.Namespace).Get(workerName(mpiJob, workerIndex))

	// If the worker Pod doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		worker := c.newWorker(mpiJob, workerIndex)
		pod, err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Create(context.TODO(), worker, metav1.CreateOptions{})
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		c.recorder.Eventf(mpiJob, corev1.EventTypeWarning, mpiJobFailedReason, "worker pod created failed: %v", err)
		return nil, err
	}
	// If the worker is not controlled by this MPIJob resource, we should log
	// a warning to the event recorder and return.
	if pod != nil && !metav1.IsControlledBy(pod, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	return pod, nil
}

func (c *MPIJobController) enqueueJobInternal(mpiJob *kubeflow.MPIJob) {
	for _, item := range c.queuedJobs {
		if getJobKey(&item.mpiJob) == getJobKey(mpiJob) {
			klog.Infof("Skipping enqueue since already in queue")
			return
		}
	}
	c.queuedJobs.Push(&Item{*mpiJob, int(*mpiJob.Spec.Priority), c.clock.Now()})
	c.jobStatus[getJobKey(mpiJob)] = queued
	klog.Infof("enqueueJobInternal called for job %s. Queue size = %d", getJobKey(mpiJob), len(c.queuedJobs))
}

func (c *MPIJobController) checkJobQueue() error {
	index := 0
	klog.Infof("checkJobQueue called, queue size = %d", len(c.queuedJobs))
	var err error
	for {
		if index == len(c.queuedJobs) {
			break
		}
		mpiJob := c.queuedJobs[index].mpiJob
		c.latestReplicas[getJobKey(&mpiJob)], err = c.calculateWorkerReplicas(&mpiJob)
		if err != nil {
			//c.enqueueJobInternal(&mpiJob)
			index += 1
			continue
		}
		c.freeSlots -= int(c.latestReplicas[getJobKey(&mpiJob)]) // This is for the workers
		c.freeSlots -= 1                                         // This one is for the launcher
		c.queue.AddRateLimited(getJobKey(&mpiJob))
		if index < len(c.queuedJobs)-1 {
			c.queuedJobs = append(c.queuedJobs[:index], c.queuedJobs[index+1:]...)
		} else {
			c.queuedJobs = c.queuedJobs[:index]
		}
	}
	return nil
}

func (c *MPIJobController) calculateWorkerReplicas(mpiJob *kubeflow.MPIJob) (int32, error) {
	// for a new job, calculate how many worker replicas to use
	worker := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	replicas := int32(math.Min(float64(c.freeSlots)-1, float64(*worker.MaxReplicas)))
	klog.Infof("calculateReplicas for %s: freeSlots = %d, replicas = %d", getJobKey(mpiJob), c.freeSlots, replicas)
	if replicas < *worker.MinReplicas {
		// shrink running jobs only when the new job cannot even run at min config

		numWorkersToFree := *worker.MinReplicas - int32(c.freeSlots) + 1
		index := len(c.runningJobs) - 1

		klog.Infof("Workers to free = %d, running jobs = %d", numWorkersToFree, len(c.runningJobs))

		var minTime time.Duration = math.MaxInt64
		for {
			if numWorkersToFree == 0 || index < 0 {
				break
			}
			it := c.runningJobs[index]
			index -= 1
			if *it.mpiJob.Spec.Priority > *mpiJob.Spec.Priority {
				break
			}
			workerPodList, err := c.getRunningWorkerPods(&it.mpiJob)
			if err != nil {
				return -1, err
			}
			jobMinReplicas := *it.mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].MinReplicas
			if int32(len(workerPodList)) > jobMinReplicas {
				if c.lastAction[getJobKey(&it.mpiJob)].Add(c.rescaleGap).Before(c.clock.Now()) {
					newPodCount := int32(math.Max(float64(jobMinReplicas), float64(len(workerPodList)-int(numWorkersToFree))))
					numWorkersToFree -= int32(len(workerPodList) - int(newPodCount))
				} else {
					minTime = min(minTime, c.lastAction[getJobKey(&it.mpiJob)].Add(c.rescaleGap).Sub(c.clock.Now()))
				}
			}
		}

		if minTime < math.MaxInt64 {
			c.queue.AddAfter(checkQueueFlag, minTime)
		}

		if numWorkersToFree > 0 {
			// queue this job
			//c.enqueueJobInternal(mpiJob)
			klog.Infof("Queued job 1 %s, %d", getJobKey(mpiJob), numWorkersToFree)
			return 0, jobQueuedError
		} else {
			numWorkersToFree = *worker.MinReplicas - int32(c.freeSlots) + 1
			index = len(c.runningJobs) - 1
			for {
				if numWorkersToFree == 0 || index < 0 {
					break
				}

				it := c.runningJobs[index]
				index -= 1

				// if the running job priority is higher than the new job
				// don't shrink it
				if *it.mpiJob.Spec.Priority > *mpiJob.Spec.Priority {
					break
				}

				if c.jobStatus[getJobKey(&it.mpiJob)] != running {
					continue
				}

				workerPodList, err := c.getRunningWorkerPods(&it.mpiJob)
				if err != nil {
					return -1, err
				}
				jobMinReplicas := *it.mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].MinReplicas
				if int32(len(workerPodList)) > jobMinReplicas && c.lastAction[getJobKey(&it.mpiJob)].Add(c.rescaleGap).Before(c.clock.Now()) {
					newPodCount := int32(math.Max(float64(jobMinReplicas),
						float64(len(workerPodList)-int(numWorkersToFree))))
					klog.Infof("Setting replicas for %s to %d", getJobKey(&it.mpiJob), newPodCount)

					err := c.sendRescaleSignal(&it.mpiJob, int32(len(workerPodList)), newPodCount)
					c.lastAction[getJobKey(&it.mpiJob)] = c.clock.Now()
					if err != nil {
						// don't remove pods if the signal failed
						continue
					}

					c.latestReplicas[getJobKey(&it.mpiJob)] = newPodCount
					numWorkersToFree -= int32(len(workerPodList) - int(newPodCount))
					c.freeSlots += len(workerPodList) - int(newPodCount)

					c.queue.AddRateLimited(getJobKey(&it.mpiJob))
				}
			}
			if numWorkersToFree > 0 {
				// queue this job
				//c.enqueueJobInternal(mpiJob)
				klog.Infof("Queued job 2 %s, %d", getJobKey(mpiJob), numWorkersToFree)
				return 0, jobQueuedError
			}
		}
		return *worker.MinReplicas, nil
	} else {
		return replicas, nil
	}
}

// getOrCreateWorkerStatefulSet gets the worker Pod controlled by this
// MPIJob, or creates one if it doesn't exist.
func (c *MPIJobController) getOrCreateWorker(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	var workerPods []*corev1.Pod
	worker := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	if worker == nil {
		return workerPods, nil
	}

	// Remove Pods when replicas are scaled down
	// TODO: shrink the job before scaling down the pods
	selector, err := workerSelector(mpiJob.Name)
	if err != nil {
		return nil, err
	}
	podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	podRunningList, err := c.getRunningWorkerPods(mpiJob)

	klog.Infof("getOrCreateWorker called for %s, latestReplicas = %d/%d, freeSlots = %d",
		getJobKey(mpiJob), c.latestReplicas[getJobKey(mpiJob)], len(podRunningList), c.freeSlots)

	// check if this is expand
	// if the expand signal is delayed, the latestReplicas won't match
	// the number of freeSlots, so we need to update latestReplicas
	//if c.jobStatus[getJobKey(mpiJob)] == running && c.latestReplicas[getJobKey(mpiJob)] > int32(len(podRunningList)) {
	//	if int(c.latestReplicas[getJobKey(mpiJob)])-len(podRunningList) > c.freeSlots {
	//		c.latestReplicas[getJobKey(mpiJob)] = int32(len(podRunningList)) + int32(c.freeSlots)
	//	}
	//}

	//if int(c.latestReplicas[getJobKey(mpiJob)])-len(podFullList) > c.freeSlots {
	//	//c.enqueueJobInternal(mpiJob)
	//	klog.Infof("Queued job from getOrCreateWorker %s, %d, %d, %d", getJobKey(mpiJob),
	//		int(c.latestReplicas[getJobKey(mpiJob)]), len(podFullList), c.freeSlots)
	//	return podFullList, jobQueuedError
	//}

	if len(podFullList) > int(c.latestReplicas[getJobKey(mpiJob)]) {
		for _, pod := range podFullList {
			indexStr, ok := pod.Labels[kubeflow.ReplicaIndexLabel]
			if !ok {
				return nil, err
			}
			index, err := strconv.Atoi(indexStr)
			if err == nil {
				if index >= int(c.latestReplicas[getJobKey(mpiJob)]) {
					err = c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	for i := 0; i < int(c.latestReplicas[getJobKey(mpiJob)]); i++ {
		pod, err := c.addWorker(mpiJob, i)
		if err != nil {
			return workerPods, err
		}
		workerPods = append(workerPods, pod)
	}

	return workerPods, nil
}

func isMPIJobSuspended(mpiJob *kubeflow.MPIJob) bool {
	return ptr.Deref(mpiJob.Spec.RunPolicy.Suspend, false)
}

func isJobSuspended(job *batchv1.Job) bool {
	return ptr.Deref(job.Spec.Suspend, false)
}

func (c *MPIJobController) removeWorker(mpiJob *kubeflow.MPIJob, index int) error {
	klog.Infof("removeWorker called for %s on idx %d", getJobKey(mpiJob), index)
	workerPrefix := mpiJob.Name + workerSuffix
	name := fmt.Sprintf("%s-%d", workerPrefix, index)
	pod, err := c.podLister.Pods(mpiJob.Namespace).Get(name)

	// If the worker Pod doesn't exist, no need to remove it.
	if errors.IsNotFound(err) {
		return nil
	}

	// If the worker is not controlled by this MPIJob resource, we should log
	// a warning to the event recorder and return.
	if pod != nil && !metav1.IsControlledBy(pod, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf(msg)
	}
	// If the worker pod is not running and cleanupPolicy is
	// set to CleanPodPolicyRunning, keep the pod.
	// Note that pending pod should still be removed under this
	// situation, since it may turn to running in the future.
	if *mpiJob.Spec.RunPolicy.CleanPodPolicy == kubeflow.CleanPodPolicyRunning && !isPodRunning(pod) && !isPodPending(pod) {
		// Keep the worker pod
		return nil
	}
	err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		klog.Errorf("Failed to delete pod[%s/%s]: %v", mpiJob.Namespace, name, err)
		return err
	}
	return nil
}

func (c *MPIJobController) deleteWorkerPods(mpiJob *kubeflow.MPIJob) error {
	var i int32 = 0
	worker := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	if worker == nil {
		return nil
	}

	for ; i < *worker.MaxReplicas; i++ {
		c.removeWorker(mpiJob, int(i))
	}
	return nil
}

func (c *MPIJobController) updateMPIJobStatus(mpiJob *kubeflow.MPIJob, launcher *batchv1.Job, worker []*corev1.Pod) error {
	oldStatus := mpiJob.Status.DeepCopy()
	if isMPIJobSuspended(mpiJob) {
		// it is suspended now
		if updateMPIJobConditions(mpiJob, kubeflow.JobSuspended, corev1.ConditionTrue, mpiJobSuspendedReason, "MPIJob suspended") {
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobSuspended", "MPIJob suspended")
		}
	} else if getCondition(mpiJob.Status, kubeflow.JobSuspended) != nil {
		// it is not suspended now, consider resumed if the condition was set before
		if updateMPIJobConditions(mpiJob, kubeflow.JobSuspended, corev1.ConditionFalse, mpiJobResumedReason, "MPIJob resumed") {
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobResumed", "MPIJob resumed")
			now := metav1.NewTime(c.clock.Now())
			mpiJob.Status.StartTime = &now
		}
	}
	launcherPodsCnt := 0
	if launcher != nil {
		launcherPods, err := c.jobPods(launcher)
		if err != nil {
			return fmt.Errorf("checking launcher pods running: %w", err)
		}
		// Job.status.Active accounts for Pending and Running pods. Count running pods
		// from the lister instead.
		launcherPodsCnt = countRunningPods(launcherPods)
		initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeLauncher)
		launcherStatus := mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher]
		launcherStatus.Failed = launcher.Status.Failed
		if isJobSucceeded(launcher) {
			launcherStatus.Succeeded = 1
			msg := fmt.Sprintf("MPIJob %s/%s successfully completed.", mpiJob.Namespace, mpiJob.Name)
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, mpiJobSucceededReason, msg)
			if mpiJob.Status.CompletionTime == nil {
				mpiJob.Status.CompletionTime = launcher.Status.CompletionTime
			}
			updateMPIJobConditions(mpiJob, kubeflow.JobSucceeded, corev1.ConditionTrue, mpiJobSucceededReason, msg)
			mpiJobsSuccessCount.Inc()
		} else if isJobFailed(launcher) {
			c.updateMPIJobFailedStatus(mpiJob, launcher, launcherPods)
		} else {
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Active = int32(launcherPodsCnt)
		}
		mpiJobInfoGauge.WithLabelValues(launcher.Name, mpiJob.Namespace).Set(1)
	}

	var (
		running = 0
		evict   = 0
	)

	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeWorker)
	//spec := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	for i := 0; i < len(worker); i++ {
		switch worker[i].Status.Phase {
		case corev1.PodFailed:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Failed += 1
			if worker[i].Status.Reason == "Evicted" {
				evict += 1
			}
		case corev1.PodSucceeded:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Succeeded += 1
		case corev1.PodRunning:
			running += 1
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Active += 1
		}
	}
	if evict > 0 {
		msg := fmt.Sprintf("%d/%d workers are evicted", evict, len(worker))
		klog.Infof("MPIJob <%s/%s>: %v", mpiJob.Namespace, mpiJob.Name, msg)
		updateMPIJobConditions(mpiJob, kubeflow.JobFailed, corev1.ConditionTrue, mpiJobEvict, msg)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, mpiJobEvict, msg)
	}

	if isMPIJobSuspended(mpiJob) {
		msg := fmt.Sprintf("MPIJob %s/%s is suspended.", mpiJob.Namespace, mpiJob.Name)
		updateMPIJobConditions(mpiJob, kubeflow.JobRunning, corev1.ConditionFalse, mpiJobSuspendedReason, msg)
	} else if launcher != nil && launcherPodsCnt >= 1 && running == len(worker) {
		msg := fmt.Sprintf("MPIJob %s/%s is running.", mpiJob.Namespace, mpiJob.Name)
		updateMPIJobConditions(mpiJob, kubeflow.JobRunning, corev1.ConditionTrue, mpiJobRunningReason, msg)
		c.recorder.Eventf(mpiJob, corev1.EventTypeNormal, "MPIJobRunning", "MPIJob %s/%s is running", mpiJob.Namespace, mpiJob.Name)
	}

	// no need to update the mpijob if the status hasn't changed since last time.
	if !reflect.DeepEqual(*oldStatus, mpiJob.Status) {
		return c.updateStatusHandler(mpiJob)
	}
	return nil
}

func (c *MPIJobController) updateMPIJobFailedStatus(mpiJob *kubeflow.MPIJob, launcher *batchv1.Job, launcherPods []*corev1.Pod) {
	jobFailedCond := getJobCondition(launcher, batchv1.JobFailed)
	reason := jobFailedCond.Reason
	if reason == "" {
		reason = mpiJobFailedReason
	}
	msg := jobFailedCond.Message
	if msg == "" {
		msg = fmt.Sprintf("MPIJob %s/%s has failed", mpiJob.Namespace, mpiJob.Name)
	}
	if reason == jobBackoffLimitExceededReason {
		// Concatenate the reason and message from the last failed Pod.
		var lastFailedPod *corev1.Pod
		for _, p := range launcherPods {
			if isPodFailed(p) && (lastFailedPod == nil || lastFailedPod.CreationTimestamp.Before(&p.CreationTimestamp)) {
				lastFailedPod = p
			}
		}
		if lastFailedPod != nil {
			reason += "/" + lastFailedPod.Status.Reason
			msg += ": " + lastFailedPod.Status.Message
			msg = truncateMessage(msg)
		}
	}
	c.recorder.Event(mpiJob, corev1.EventTypeWarning, reason, msg)
	if mpiJob.Status.CompletionTime == nil {
		now := metav1.Now()
		mpiJob.Status.CompletionTime = &now
	}
	updateMPIJobConditions(mpiJob, kubeflow.JobFailed, corev1.ConditionTrue, reason, msg)
	mpiJobsFailureCount.Inc()
}

// When a mpiJob is added, set the defaults and enqueue the current mpiJob.
func (c *MPIJobController) addMPIJob(obj interface{}) {
	mpiJob := obj.(*kubeflow.MPIJob)

	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)
	c.enqueueMPIJob(mpiJob)
}

// enqueueMPIJob takes a MPIJob resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than MPIJob.
func (c *MPIJobController) enqueueMPIJob(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.queue.AddRateLimited(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the MPIJob resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that MPIJob resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *MPIJobController) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	klog.V(4).Infof("Processing object: %s", object.GetName())
	ownerRef, ownerGVK, err := ownerReferenceAndGVK(object)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	// If the Pod is controlled by a Job, get the Job's ownerReference.
	if ownerGVK.Group == batchv1.GroupName && ownerGVK.Kind == "Job" {
		j, err := c.jobLister.Jobs(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			runtime.HandleError(fmt.Errorf("obtaining owning k8s Job: %w", err))
			return
		}
		ownerRef, ownerGVK, err = ownerReferenceAndGVK(j)
		if err != nil {
			runtime.HandleError(fmt.Errorf("obtaining k8s Job's owner: %w", err))
			return
		}
	}

	// Compare the OwnerReference Group and Kind against the OwnerType Group and Kind.
	// Since we do not support conversion webhook now, we do not deal with v1alpha1/v1alpha2/v1 resources in this operator.
	if ownerGVK.Kind != kubeflow.Kind || ownerGVK.Group != kubeflow.GroupName || ownerGVK.Version != kubeflow.GroupVersion {
		return
	}

	mpiJob, err := c.mpiJobLister.MPIJobs(object.GetNamespace()).Get(ownerRef.Name)
	if err != nil {
		klog.V(4).Infof("ignoring orphaned object '%s' of mpi job '%s'", object.GetSelfLink(), ownerRef.Name)
		return
	}

	c.enqueueMPIJob(mpiJob)
}

func (c *MPIJobController) handleObjectUpdate(old, new interface{}) {
	oldObj := old.(metav1.Object)
	newObj := new.(metav1.Object)
	if newObj.GetResourceVersion() == oldObj.GetResourceVersion() {
		// Periodic re-sync will send update events for all known
		// ConfigMaps. Two different versions of the same ConfigMap
		// will always have different RVs.
		return
	}
	c.handleObject(new)
}

// doUpdateJobStatus updates the status of the given MPIJob by call apiServer.
func (c *MPIJobController) doUpdateJobStatus(mpiJob *kubeflow.MPIJob) error {
	_, err := c.kubeflowClient.KubeflowV2beta1().MPIJobs(mpiJob.Namespace).UpdateStatus(context.TODO(), mpiJob, metav1.UpdateOptions{})
	return err
}

// newConfigMap creates a new ConfigMap containing configurations for an MPIJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the MPIJob resource that 'owns' it.
func newConfigMap(mpiJob *kubeflow.MPIJob, workerReplicas int32, workerPods []*corev1.Pod) *corev1.ConfigMap {
	var buffer bytes.Buffer
	slots := ptr.Deref(mpiJob.Spec.SlotsPerWorker, 1)
	// note that pod.spec.dnsConfig also affect the svc resolution
	// ref: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
	// launcher can be reach with hostname or service name
	if ptr.Deref(mpiJob.Spec.RunLauncherAsWorker, false) {
		//buffer.WriteString(fmt.Sprintf("host %s.%s ++cpus %d\n", name, mpiJob.Name, slots))
		buffer.WriteString(fmt.Sprintf("localhost slots=%d\n", slots))
		/*switch mpiJob.Spec.MPIImplementation {
		case kubeflow.MPIImplementationOpenMPI:
			buffer.WriteString(fmt.Sprintf("%s.%s.%s.svc slots=%d\n", name, mpiJob.Name, mpiJob.Namespace, slots))
		case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
			buffer.WriteString(fmt.Sprintf("%s.%s.%s.svc:%d\n", name, mpiJob.Name, mpiJob.Namespace, slots))
		}*/
	}

	for i := 0; i < int(*mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].MaxReplicas); i++ {
		name := workerName(mpiJob, i)

		// Find the corresponding pod for this worker
		var podIP string
		for _, pod := range workerPods {
			if pod.Name == name && pod.Status.PodIP != "" {
				podIP = pod.Status.PodIP
				break
			}
		}

		// Use IP address if available, otherwise fall back to DNS name
		if podIP != "" {
			buffer.WriteString(fmt.Sprintf("%s slots=%d\n", podIP, slots))
		} else {
			// Fallback to DNS name if IP is not available
			buffer.WriteString(fmt.Sprintf("%s.%s.%s.svc slots=%d\n", name, mpiJob.Name, mpiJob.Namespace, slots))
		}
		/*switch mpiJob.Spec.MPIImplementation {
		case kubeflow.MPIImplementationOpenMPI:
			buffer.WriteString(fmt.Sprintf("%s.%s.%s.svc slots=%d\n", name, mpiJob.Name, mpiJob.Namespace, slots))
		case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
			buffer.WriteString(fmt.Sprintf("%s.%s.%s.svc:%d\n", name, mpiJob.Name, mpiJob.Namespace, slots))
		}*/
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mpiJob.Name + configSuffix,
			Namespace: mpiJob.Namespace,
			Labels: map[string]string{
				"app": mpiJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Data: map[string]string{
			hostfileName: buffer.String(),
		},
	}
}

// updateDiscoverHostsInConfigMap updates the ConfigMap if the content of `discover_hosts.sh` changes.
func updateDiscoverHostsInConfigMap(configMap *corev1.ConfigMap, mpiJob *kubeflow.MPIJob, runningPods []*corev1.Pod) {
	// Sort the slice of Pods to make sure the order of entries in `discover_hosts.sh` is maintained.
	sort.Slice(runningPods, func(i, j int) bool {
		return runningPods[i].Name < runningPods[j].Name
	})

	var buffer bytes.Buffer
	buffer.WriteString("#!/bin/sh\n")

	// We don't check if launcher is running here, launcher should always be there or the job failed
	if ptr.Deref(mpiJob.Spec.RunLauncherAsWorker, false) {
		name := mpiJob.Name + launcherSuffix
		buffer.WriteString(fmt.Sprintf("echo %s.%s.%s.svc\n", name, mpiJob.Name, mpiJob.Namespace))
	}

	for _, p := range runningPods {
		buffer.WriteString(fmt.Sprintf("echo %s.%s.%s.svc\n", p.Name, mpiJob.Name, p.Namespace))
	}

	configMap.Data[discoverHostsScriptName] = buffer.String()
}

// newJobService creates a Service with the same name of Job for both launcher and worker pods
func newJobService(job *kubeflow.MPIJob) *corev1.Service {
	labels := map[string]string{
		kubeflow.OperatorNameLabel: kubeflow.OperatorName,
		kubeflow.JobNameLabel:      job.Name,
	}
	return newService(job, job.Name, labels)
}

func newService(job *kubeflow.MPIJob, name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selector,
		},
	}
}

// newSSHAuthSecret creates a new Secret that holds SSH auth: a private Key
// and its public key version.
func newSSHAuthSecret(job *kubeflow.MPIJob) (*corev1.Secret, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating private SSH key: %w", err)
	}
	privateDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("converting private SSH key to DER format: %w", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privateDER,
	})

	publicKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("generating public SSH key: %w", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + sshAuthSecretSuffix,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflow.SchemeGroupVersionKind),
			},
		},
		Type: corev1.SecretTypeSSHAuth,
		Data: map[string][]byte{
			corev1.SSHAuthPrivateKey: privatePEM,
			sshPublicKey:             ssh.MarshalAuthorizedKey(publicKey),
		},
	}, nil
}

func workerName(mpiJob *kubeflow.MPIJob, index int) string {
	return fmt.Sprintf("%s%s-%d", mpiJob.Name, workerSuffix, index)
}

// newWorker creates a new worker Pod for an MPIJob resource. It also
// sets the appropriate OwnerReferences on the resource so handleObject can
// discover the MPIJob resource that 'owns' it.
func (c *MPIJobController) newWorker(mpiJob *kubeflow.MPIJob, index int) *corev1.Pod {
	name := workerName(mpiJob, index)

	podTemplate := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker].Template.DeepCopy()

	// keep the labels which are set in PodTemplate
	if len(podTemplate.Labels) == 0 {
		podTemplate.Labels = make(map[string]string)
	}
	for key, value := range defaultLabels(mpiJob.Name, worker) {
		podTemplate.Labels[key] = value
	}
	podTemplate.Labels[kubeflow.ReplicaIndexLabel] = strconv.Itoa(index)
	podTemplate.Spec.Hostname = name
	podTemplate.Spec.Subdomain = mpiJob.Name // Matches job' Service name.

	launcherMatch := make(map[string]string)
	launcherMatch[kubeflow.OperatorNameLabel] = kubeflow.OperatorName
	launcherMatch[kubeflow.JobNameLabel] = mpiJob.Name
	launcherMatch[kubeflow.JobRoleLabel] = launcher

	workerMatch := make(map[string]string)
	workerMatch[kubeflow.OperatorNameLabel] = kubeflow.OperatorName
	workerMatch[kubeflow.JobNameLabel] = mpiJob.Name
	workerMatch[kubeflow.JobRoleLabel] = worker

	schedulingAffinity := make([]corev1.WeightedPodAffinityTerm, 0)
	schedulingAffinity = append(schedulingAffinity, corev1.WeightedPodAffinityTerm{
		Weight: 50,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: launcherMatch,
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	})
	schedulingAffinity = append(schedulingAffinity, corev1.WeightedPodAffinityTerm{
		Weight: 100,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: workerMatch,
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	})

	if podTemplate.Spec.Affinity == nil {
		podTemplate.Spec.Affinity = &corev1.Affinity{
			PodAffinity: &corev1.PodAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: schedulingAffinity,
			},
		}
	}

	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	// The Intel and MPICH implementations require workers to communicate with the launcher through its hostname.
	searche := fmt.Sprintf("%s.%s.svc.cluster.local", mpiJob.Name, mpiJob.Namespace)
	if podTemplate.Spec.DNSConfig == nil {
		podTemplate.Spec.DNSConfig = &corev1.PodDNSConfig{Searches: []string{searche}}
	} else {
		podTemplate.Spec.DNSConfig.Searches = append(podTemplate.Spec.DNSConfig.Searches, searche)
	}
	setRestartPolicy(podTemplate, mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker])

	container := &podTemplate.Spec.Containers[0]
	if len(container.Command) == 0 && len(container.Args) == 0 {
		container.Command = []string{"/usr/sbin/sshd", "-De"}
	}
	container.Env = append(container.Env, workerEnvVars...)
	c.setupSSHOnPod(&podTemplate.Spec, mpiJob)

	// add SchedulerName to podSpec
	if c.PodGroupCtrl != nil {
		c.PodGroupCtrl.decoratePodTemplateSpec(podTemplate, mpiJob.Name)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   mpiJob.Namespace,
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: podTemplate.Spec,
	}
}

func (c *MPIJobController) newLauncherJob(mpiJob *kubeflow.MPIJob, numWorkers int) *batchv1.Job {
	klog.Infof("Creating launcher job for %s with %d workers", getJobKey(mpiJob), numWorkers)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mpiJob.Name + launcherSuffix,
			Namespace: mpiJob.Namespace,
			Labels: map[string]string{
				"app": mpiJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: mpiJob.Spec.RunPolicy.TTLSecondsAfterFinished,
			ActiveDeadlineSeconds:   mpiJob.Spec.RunPolicy.ActiveDeadlineSeconds,
			BackoffLimit:            mpiJob.Spec.RunPolicy.BackoffLimit,
			Template:                c.newLauncherPodTemplate(mpiJob, numWorkers),
		},
	}
	if isMPIJobSuspended(mpiJob) {
		job.Spec.Suspend = ptr.To(true)
	}
	return job
}

// newLauncherPodTemplate creates a new launcher Job for an MPIJob resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the MPIJob resource that 'owns' it.
func (c *MPIJobController) newLauncherPodTemplate(mpiJob *kubeflow.MPIJob, numWorkers int) corev1.PodTemplateSpec {
	launcherName := mpiJob.Name + launcherSuffix

	podTemplate := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.DeepCopy()
	// copy the labels and annotations to pod from PodTemplate
	if len(podTemplate.Labels) == 0 {
		podTemplate.Labels = make(map[string]string)
	}
	for key, value := range defaultLabels(mpiJob.Name, launcher) {
		podTemplate.Labels[key] = value
	}
	// add SchedulerName to podSpec
	if c.PodGroupCtrl != nil {
		c.PodGroupCtrl.decoratePodTemplateSpec(podTemplate, mpiJob.Name)
	}
	podTemplate.Spec.Hostname = launcherName
	podTemplate.Spec.Subdomain = mpiJob.Name // Matches job' Service name.
	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	container := &podTemplate.Spec.Containers[0]
	container.Env = append(container.Env, launcherEnvVars...)
	container.Args = append([]string{fmt.Sprint("+p", numWorkers+1)}, container.Args...)
	container.Args = append(container.Args, "++nodelist", configMountPath+"/"+hostfileName, "++server", "++server-port", fmt.Sprint(ccsPort))
	slotsStr := strconv.Itoa(int(*mpiJob.Spec.SlotsPerWorker))
	switch mpiJob.Spec.MPIImplementation {
	case kubeflow.MPIImplementationOpenMPI:
		container.Env = append(container.Env, ompiEnvVars...)
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  openMPISlotsEnv,
			Value: slotsStr,
		})
	case kubeflow.MPIImplementationIntel:
		container.Env = append(container.Env, intelEnvVars...)
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  intelMPISlotsEnv,
			Value: slotsStr,
		})
	case kubeflow.MPIImplementationMPICH:
		container.Env = append(container.Env, mpichEnvVars...)
	}
	if !ptr.Deref(mpiJob.Spec.RunLauncherAsWorker, false) {
		container.Env = append(container.Env,
			// We overwrite these environment variables so that users will not
			// be mistakenly using GPU resources for launcher due to potential
			// issues with scheduler/container technologies.
			nvidiaDisableEnvVars...)
	}
	c.setupSSHOnPod(&podTemplate.Spec, mpiJob)

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the replica level.
	if podTemplate.Spec.RestartPolicy != "" {
		errMsg := "Restart policy in pod template overridden by restart policy in replica spec"
		klog.Warning(errMsg)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	setRestartPolicy(podTemplate, mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher])

	podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes,
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: mpiJob.Name + configSuffix,
					},
					Items: configVolumeItems,
				},
			},
		})
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      configVolumeName,
		MountPath: configMountPath,
	})

	workerMatch := make(map[string]string)
	workerMatch[kubeflow.OperatorNameLabel] = kubeflow.OperatorName
	workerMatch[kubeflow.JobNameLabel] = mpiJob.Name
	workerMatch[kubeflow.JobRoleLabel] = worker

	schedulingAffinity := make([]corev1.WeightedPodAffinityTerm, 0)
	schedulingAffinity = append(schedulingAffinity, corev1.WeightedPodAffinityTerm{
		Weight: 100,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: workerMatch,
			},
			TopologyKey: "topology.kubernetes.io/zone",
		},
	})

	if podTemplate.Spec.Affinity == nil {
		podTemplate.Spec.Affinity = &corev1.Affinity{
			PodAffinity: &corev1.PodAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: schedulingAffinity,
			},
		}
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: podTemplate.Spec,
	}
}

func (c *MPIJobController) jobPods(j *batchv1.Job) ([]*corev1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(j.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("parsing Pod selector: %w", err)
	}
	pods, err := c.podLister.Pods(j.Namespace).List(selector)
	if err != nil {
		return nil, fmt.Errorf("obtaining pods: %w", err)
	}
	var result = make([]*corev1.Pod, 0, len(pods))
	for _, p := range pods {
		if metav1.IsControlledBy(p, j) {
			result = append(result, p)
		}
	}
	return result, nil
}

func countRunningPods(pods []*corev1.Pod) int {
	running := 0
	for _, p := range pods {
		if isPodRunning(p) {
			running++
		}
	}
	return running
}

func setRestartPolicy(podTemplateSpec *corev1.PodTemplateSpec, spec *kubeflow.ReplicaSpec) {
	if spec.RestartPolicy == kubeflow.RestartPolicyExitCode {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicyNever
	} else {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicy(spec.RestartPolicy)
	}
}

func isJobFinished(j *batchv1.Job) bool {
	return isJobSucceeded(j) || isJobFailed(j)
}

func isJobFailed(j *batchv1.Job) bool {
	c := getJobCondition(j, batchv1.JobFailed)
	return c != nil && c.Status == corev1.ConditionTrue
}

func isJobSucceeded(j *batchv1.Job) bool {
	c := getJobCondition(j, batchv1.JobComplete)
	return c != nil && c.Status == corev1.ConditionTrue
}

func getJobCondition(j *batchv1.Job, condition batchv1.JobConditionType) *batchv1.JobCondition {
	for _, c := range j.Status.Conditions {
		if c.Type == condition {
			return &c
		}
	}
	return nil
}

func isPodRunning(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning
}

func isPodPending(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodPending
}

func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

func isCleanUpPods(cleanPodPolicy *kubeflow.CleanPodPolicy) bool {
	if *cleanPodPolicy == kubeflow.CleanPodPolicyAll || *cleanPodPolicy == kubeflow.CleanPodPolicyRunning {
		return true
	}
	return false
}

func defaultLabels(jobName, role string) map[string]string {
	return map[string]string{
		kubeflow.OperatorNameLabel: kubeflow.OperatorName,
		kubeflow.JobNameLabel:      jobName,
		kubeflow.JobRoleLabel:      role,
	}
}

func workerSelector(mpiJobName string) (labels.Selector, error) {
	set := defaultLabels(mpiJobName, worker)
	return labels.ValidatedSelectorFromSet(set)
}

func (c *MPIJobController) workerReplicas(job *kubeflow.MPIJob) int32 {
	return c.latestReplicas[getJobKey(job)]
}

func (c *MPIJobController) setupSSHOnPod(podSpec *corev1.PodSpec, job *kubeflow.MPIJob) {
	var mode *int32
	if job.Spec.SSHAuthMountPath == rootSSHPath {
		mode = ptr.To[int32](0600)
	}
	mainContainer := &podSpec.Containers[0]
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: sshAuthVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					DefaultMode: mode,
					SecretName:  job.Name + sshAuthSecretSuffix,
					Items:       sshVolumeItems,
				},
			},
		})

	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts,
		corev1.VolumeMount{
			Name:      sshAuthVolume,
			MountPath: job.Spec.SSHAuthMountPath,
		})
}

func ownerReferenceAndGVK(object metav1.Object) (*metav1.OwnerReference, schema.GroupVersionKind, error) {
	ownerRef := metav1.GetControllerOf(object)
	if ownerRef == nil {
		return nil, schema.GroupVersionKind{}, nil
	}
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	if err != nil {
		return nil, schema.GroupVersionKind{}, fmt.Errorf("parsing owner's API version: %w", err)
	}
	return ownerRef, gv.WithKind(ownerRef.Kind), nil
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	if len(message) <= eventMessageLimit {
		return message
	}
	suffix := "..."
	return message[:eventMessageLimit-len(suffix)] + suffix
}
