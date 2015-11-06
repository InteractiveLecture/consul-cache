package servicecache

import (
	"errors"
	"fmt"
	"github.com/hashicorp/consul/api"
	"log"
	"math/rand"
	"sync"
	"time"
)

type ServiceRetriever func(consulAddress string) (map[string]*api.AgentService, error)

type ConsulCache struct {
	*sync.RWMutex
	serviceMap       map[string][]*api.AgentService
	alreadyRunning   bool
	ticker           *time.Ticker
	consulAddress    string
	abortChan        chan bool
	ErrorChan        chan error
	SuccessChan      chan bool
	serviceRetriever ServiceRetriever
	refreshIntervall time.Duration
}

var instance = &ConsulCache{}

func (cache *ConsulCache) SetServiceRetriever(r ServiceRetriever) error {
	if cache.alreadyRunning {
		return errors.New("can't change retriever on running cache")
	}
	cache.serviceRetriever = r
	return nil
}

func getFromServer(consulAddress string) (map[string]*api.AgentService, error) {
	client, consulErr := getClient(consulAddress)
	if consulErr != nil {
		return nil, consulErr
	}
	catalog := client.Agent()
	services, err := catalog.Services()
	if err != nil {
		return nil, err
	}
	return services, nil
}

func Stop() bool {
	return instance.Stop()
}

func (cache *ConsulCache) Stop() bool {

	if !cache.alreadyRunning {
		return false
	}
	cache.alreadyRunning = false
	cache.ticker.Stop()
	select {
	case cache.abortChan <- true:
		break
	default:
		break
	}
	return true
}

func AlreadyRunning() bool {
	return instance.AlreadyRunning()
}

func (cache *ConsulCache) AlreadyRunning() bool {
	return cache.alreadyRunning
}

func IsWatched(services ...string) bool {
	return instance.IsWatched(services...)
}

func (cache *ConsulCache) IsWatched(services ...string) bool {
	for _, service := range services {
		_, ok := cache.serviceMap[service]
		if !ok {
			return false
		}
	}
	return true
}

func GetInstance() *ConsulCache {
	return instance
}

func Configure(consulAddress string, refreshIntervall time.Duration, services ...string) (*ConsulCache, error) {
	if instance.alreadyRunning {
		return nil, errors.New("cannot configure running cache.")
	}
	instance.RWMutex = new(sync.RWMutex)
	instance.consulAddress = consulAddress
	instance.serviceRetriever = getFromServer
	instance.serviceMap = make(map[string][]*api.AgentService, 0)
	instance.SuccessChan = make(chan bool)
	instance.ErrorChan = make(chan error)
	instance.WatchServices(services...)
	instance.refreshIntervall = refreshIntervall
	return instance, nil
}

func Start(maxRetries int, retryTimeout time.Duration) error {
	return instance.Start(maxRetries, retryTimeout)
}

func (cache *ConsulCache) Start(maxRetries int, retryTimeout time.Duration) error {
	err := cache.RefreshAndRestart()
	if err != nil {
		for i := 0; i < maxRetries; i++ {
			select {
			case err := <-cache.ErrorChan:
				log.Println(err)
				time.Sleep(retryTimeout)
			case running := <-cache.SuccessChan:
				cache.alreadyRunning = running
			}
			if cache.alreadyRunning {
				break
			}
		}
	} else {
		cache.alreadyRunning = true
	}
	if !cache.alreadyRunning {
		return errors.New("unable to start cache.")
	}
	return nil
}

func RefreshAndRestart() error {
	return instance.RefreshAndRestart()
}

func (cache *ConsulCache) RefreshAndRestart() error {
	err := cache.Refresh()
	cache.RestartTicker()
	return err
}

func (cache *ConsulCache) RestartTicker() {
	if cache.ticker != nil {
		cache.ticker.Stop()
		select {
		case cache.abortChan <- true:
			break
		default:
			break
		}
	}
	cache.ticker = time.NewTicker(cache.refreshIntervall)
	go tickerLoop(cache)
}

func tickerLoop(cache *ConsulCache) {
	breakLoop := false
	for {
		select {
		case <-cache.ticker.C:
			err := cache.Refresh()
			if err != nil {
				cache.ErrorChan <- err
				continue
			}
			cache.SuccessChan <- true
		case breakLoop = <-cache.abortChan:
			break
		}
		if breakLoop {
			break
		}
	}
}

func (cache *ConsulCache) verifyResult() error {
	for key, service := range cache.serviceMap {
		if len(service) < 1 {
			return errors.New(fmt.Sprintf("could not refresh %s", key))
		}
	}
	return nil

}

func Refresh() error {
	return instance.Refresh()
}

func (cache *ConsulCache) Refresh() error {
	services, err := cache.serviceRetriever(cache.consulAddress)
	if err != nil {
		return err
	}
	cache.Lock()
	defer cache.Unlock()
	cache.clear()
	for _, service := range services {
		if val, ok := cache.serviceMap[service.Service]; ok {
			cache.serviceMap[service.Service] = append(val, service)
		}
	}
	return cache.verifyResult()
}

func (cache *ConsulCache) clear() {
	for k, _ := range cache.serviceMap {
		cache.serviceMap[k] = nil
	}
}

func WatchServices(serviceNames ...string) {
	instance.WatchServices(serviceNames...)
}

func (cache *ConsulCache) WatchServices(serviceNames ...string) {
	cache.Lock()
	defer cache.Unlock()
	for _, service := range serviceNames {
		cache.serviceMap[service] = make([]*api.AgentService, 0)
	}
}

func GetServiceAddress(serviceName string) (string, error) {
	return instance.GetServiceAddress(serviceName)
}

func (cache *ConsulCache) GetServiceAddress(serviceName string) (string, error) {
	instance, err := cache.GetServiceInstance(serviceName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", instance.Address, instance.Port), nil
}

func GetServiceInstance(serviceName string) (*api.AgentService, error) {
	return instance.GetServiceInstance(serviceName)
}

func (cache *ConsulCache) GetServiceInstance(serviceName string) (*api.AgentService, error) {
	cache.RLock()
	val, ok := cache.serviceMap[serviceName]
	cache.RUnlock()
	if ok {
		if len(val) == 0 {
			log.Printf("initial request for service %s\n", serviceName)
			cache.Refresh()
			cache.RLock()
			val, ok = cache.serviceMap[serviceName]
			cache.RUnlock()
			if len(val) == 0 {
				return nil, errors.New("Requested Service is not avaiable")
			}
		}
		//return random service
		return val[rand.Intn(len(val))], nil

	}
	return nil, errors.New("Requested unregistered service")
}

func getClient(address string) (*api.Client, error) {
	config := api.DefaultConfig()
	config.Address = address
	client, consulError := api.NewClient(config)
	if consulError != nil {
		return client, consulError
	}
	return client, nil
}
