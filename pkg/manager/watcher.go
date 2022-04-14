package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/dispatcher/pkg/storeset/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
	"time"
)

func StartListWatcher(ctx context.Context) error {
	var config *rest.Config
	home := homedir.HomeDir()
	join := filepath.Join(home, ".kube", "config")
	_, err := os.Stat(join)
	if err == nil {
		config, err = clientcmd.BuildConfigFromFlags("", join)
		if err != nil {
			return err
		}
	} else {
		if os.IsNotExist(err) {
			config, err = rest.InClusterConfig()
			if err != nil {
				return err
			}
		}
	}
	if config == nil {
		return fmt.Errorf("kubeconfig not found")
	}

	newSelector, err := convertLabelSelector()
	if err != nil {
		return err
	}

	listOps := metav1.ListOptions{
		LabelSelector: newSelector.String(),
		//ISSUE: https://github.com/kubernetes/kubernetes/issues/51046
		//FieldSelector: fields.OneTermEqualSelector("status.ready", "true").String(),
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	gvr := schema.GroupVersionResource{Group: "core.stream-stack.tanx", Version: "v1", Resource: "storesets"}
	informerFactory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynamicClient, time.Hour, "", func(options *metav1.ListOptions) {
		options.LabelSelector = listOps.LabelSelector
	})
	resource := informerFactory.ForResource(gvr)
	handler := &storesetResourceEventHandler{ctx: ctx}
	resource.Informer().AddEventHandler(handler)

	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	return nil
}

type storesetResourceEventHandler struct {
	ctx context.Context
}

func (s *storesetResourceEventHandler) OnAdd(obj interface{}) {
	store, err := convert(obj)
	if err != nil {
		logrus.Errorf("convert k8s storeset(crd) to storeset error:%v", err)
		return
	}
	if store == nil {
		return
	}
	StoreSetConnOperation <- func(m map[string]*StoreSetConn) {
		conn := GetOrCreateConn(s.ctx, m, store)
		conn.OpCh <- subscribePartition
	}
}

func (s *storesetResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	s.OnDelete(oldObj)
	s.OnAdd(newObj)
}

func (s *storesetResourceEventHandler) OnDelete(obj interface{}) {
	fmt.Println("OnDelete", obj)
	store, err := convert(obj)
	if err != nil {
		logrus.Errorf("convert k8s storeset(crd) to storeset error:%v", err)
		return
	}
	if store == nil {
		return
	}
	StoreSetConnOperation <- func(m map[string]*StoreSetConn) {
		conn := GetOrCreateConn(s.ctx, m, store)
		conn.Stop()
	}
}

func convert(obj interface{}) (*storeset, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("Cast type conversion failed")
	}
	set := &v1.StoreSet{}
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.UnstructuredContent(), set)
	if err != nil {
		logrus.Errorf("convert Unstructured to storeset error:%v", err)
		return nil, err
	}
	if set.Status.Status != "ready" {
		return nil, nil
	}
	store := NewStoreset(set)
	return store, nil
}

func NewStoreset(set *v1.StoreSet) *storeset {
	s := &storeset{
		Name:      set.Name,
		Namespace: set.Namespace,
		Uris:      buildStoreUri(set),
	}
	return s
}

func buildStoreUri(item *v1.StoreSet) []string {
	replicas := *item.Spec.Store.Replicas
	addrs := make([]string, replicas)
	var i int32
	for ; i < replicas; i++ {
		//TODO:重构名称的生成,应该和模板统一,使用template的自定义函数
		addrs[i] = fmt.Sprintf(`%s-%d.%s.%s:%s`, item.Name, i, item.Status.StoreStatus.ServiceName, item.Namespace, item.Spec.Store.Port)
	}
	return addrs
}

type storeset struct {
	Name      string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Namespace string   `protobuf:"bytes,2,opt,name=Namespace,proto3" json:"Namespace,omitempty"`
	Uris      []string `protobuf:"bytes,3,rep,name=uris,proto3" json:"uris,omitempty"`
}

func convertLabelSelector() (labels.Selector, error) {
	if len(selector) == 0 {
		return labels.Everything(), nil
	}
	metaSelector := &metav1.LabelSelector{}
	if err := json.Unmarshal([]byte(selector), metaSelector); err != nil {
		return nil, err
	}
	newSelector := labels.NewSelector()
	for k, v := range metaSelector.MatchLabels {
		requirement, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			return nil, err
		}
		newSelector.Add(*requirement)
	}
	for _, expression := range metaSelector.MatchExpressions {
		var op selection.Operator
		switch expression.Operator {
		case metav1.LabelSelectorOpIn:
			op = selection.In
		case metav1.LabelSelectorOpNotIn:
			op = selection.NotIn
		case metav1.LabelSelectorOpExists:
			op = selection.Exists
		case metav1.LabelSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		}

		requirement, err := labels.NewRequirement(expression.Key, op, expression.Values)
		if err != nil {
			return nil, err
		}
		newSelector.Add(*requirement)
	}
	return newSelector, nil
}
