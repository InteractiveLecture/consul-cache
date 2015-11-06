package servicecache

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"strconv"
	"testing"
	"time"
)

func TestCacheConfiguration(t *testing.T) {
	fmt.Println("TestCacheConfiguration")
	cache, err := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	if err != nil {
		t.Error("expected no error got ", err)
	}

	if !(cache.IsWatched("authentication-service", "acl-service")) {
		t.Error("expected authentication-service and acl-service to be watched.")
	}

	if cache.AlreadyRunning() {
		t.Error("the cache should not be started yet")
	}

	fmt.Println("TestCacheConfiguration done!")
}

func TestRefreshOk(t *testing.T) {

	fmt.Println("TestRefreshCacheOk")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	err := cache.SetServiceRetriever(fakeRetriever)
	if err != nil {
		t.Error("expected no error, got ", err)
	}
	err = cache.Refresh()
	if err != nil {
		t.Error("expected no error, got ", err)
	}

	fmt.Println("TestRefreshCacheOk done")
}

func TestRefreshFail(t *testing.T) {

	fmt.Println("TestRefreshCacheFail done")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service", "document-service")
	err := cache.SetServiceRetriever(fakeRetriever)
	if err != nil {
		t.Error("expected no error, got ", err)
	}
	err = cache.Refresh()
	if err == nil {
		t.Error("expected error, but no one was returned")
	}

	fmt.Println("TestRefreshCacheFail done")
}

func TestStartStop(t *testing.T) {

	fmt.Println("TestStartStop ")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	err := cache.SetServiceRetriever(fakeRetriever)
	if err != nil {
		t.Error("expected no error, got ", err)
	}

	err = cache.Start(3, 1*time.Millisecond)
	if err != nil {
		t.Error("encountered error while starting cache: ", err)
	}
	if !cache.AlreadyRunning() {
		t.Error("cache should be running right now!")
	}
	result := cache.Stop()
	if !result {
		t.Error("stop was unsuccessfull")
	}
	fmt.Println("TestStartStop done")
}

func TestGetServiceInstance(t *testing.T) {

	fmt.Println("TestGetServiceInstance")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	err := cache.SetServiceRetriever(fakeRetriever)
	if err != nil {
		t.Error("expected no error, got ", err)
	}

	err = cache.Refresh()
	if err != nil {
		t.Error("expected no error, got ", err)
	}
	instances := make(map[string]bool)
	for i := 0; i < 4000; i++ {
		service, instanceErr := cache.GetServiceInstance("authentication-service")
		if instanceErr != nil {
			t.Error("expected no error, but got ", instanceErr)
		}
		instances[service.Address] = true
		if len(instances) > 2 {
			break
		}
	}
	if len(instances) < 3 {
		t.Error("expected each instance to get hit at least once.")
	}

	fmt.Println("TestGetServiceInstance done")
}
func TestGetServiceAddress(t *testing.T) {

	fmt.Println("TestGetServiceAddress")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	err := cache.SetServiceRetriever(fakeRetriever)
	if err != nil {
		t.Error("expected no error, got ", err)
	}

	err = cache.Refresh()
	if err != nil {
		t.Error("expected no error, got ", err)
	}
	instances := make(map[string]bool)
	for i := 0; i < 4000; i++ {
		address, instanceErr := cache.GetServiceAddress("authentication-service")
		if instanceErr != nil {
			t.Error("expected no error, but got ", instanceErr)
		}
		instances[address] = true
		if len(instances) > 2 {
			break
		}
	}
	if len(instances) < 3 {
		t.Error("expected each instance to get hit at least once.")
	}
	for i := 1; i < 4; i++ {
		expectedAddress := fmt.Sprintf("192.168.2.%d:80", i)
		if !instances[expectedAddress] {
			t.Error("expected ", expectedAddress, " but could not find in result")
		}
	}

	fmt.Println("TestGetServiceAddress done")
}

func createService(id string, service string, port int, address string, tags ...string) *api.AgentService {
	return &api.AgentService{
		ID:      id,
		Service: service,
		Tags:    tags,
		Port:    port,
		Address: address,
	}

}

func fakeRetriever(consulAddress string) (map[string]*api.AgentService, error) {
	services := make(map[string]*api.AgentService)

	for i := 1; i < 4; i++ {
		index := strconv.Itoa(i)
		services[index] = createService(index, "authentication-service", 80, fmt.Sprintf("192.168.2.%s", index), "interactive-lecture", "public")
	}
	for i := 4; i < 7; i++ {
		index := strconv.Itoa(i)
		services[index] = createService(index, "acl-service", 8080, fmt.Sprintf("192.168.2.%s", index), "interactive-lecture", "public")
	}

	for i := 7; i < 10; i++ {
		index := strconv.Itoa(i)
		services[index] = createService(index, "lecture-service", 80, fmt.Sprintf("192.168.2.%s", index), "interactive-lecture", "public")
	}
	return services, nil
}
