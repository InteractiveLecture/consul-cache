package servicecache

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func prepare(t *testing.T) *ConsulCache {
	cache, err := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service")
	assert.Nil(t, err)
	err = cache.SetServiceRetriever(fakeRetriever)
	assert.Nil(t, err)
	return cache
}

func TestConvenientFunctions(t *testing.T) {
	_ = prepare(t)
	assert.True(t, IsWatched("authentication-service", "acl-service"))
	assert.False(t, AlreadyRunning())
	assert.Nil(t, Start(3, 1*time.Millisecond))
	result, _ := GetServiceAddress("authentication-service")
	assert.NotNil(t, result)
	assert.True(t, Stop())
}

func TestCacheConfiguration(t *testing.T) {
	fmt.Println("TestCacheConfiguration")
	cache := prepare(t)
	assert.True(t, cache.IsWatched("authentication-service", "acl-service"))
	assert.False(t, cache.AlreadyRunning())
	fmt.Println("TestCacheConfiguration done!")
}

func TestRefreshOk(t *testing.T) {
	fmt.Println("TestRefreshCacheOk")
	cache := prepare(t)
	assert.Nil(t, cache.Refresh())
	fmt.Println("TestRefreshCacheOk done")
}

func TestRefreshFail(t *testing.T) {
	fmt.Println("TestRefreshCacheFail done")
	cache, _ := Configure("discovery:8500", 1*time.Millisecond, "authentication-service", "acl-service", "document-service")
	assert.Nil(t, cache.SetServiceRetriever(fakeRetriever))
	assert.NotNil(t, cache.Refresh())
	fmt.Println("TestRefreshCacheFail done")
}

func TestStartStop(t *testing.T) {
	fmt.Println("TestStartStop ")
	cache := prepare(t)
	assert.Nil(t, cache.Start(3, 1*time.Millisecond))
	assert.True(t, cache.AlreadyRunning())
	assert.True(t, cache.Stop())
	fmt.Println("TestStartStop done")
}

func TestGetServiceInstance(t *testing.T) {
	fmt.Println("TestGetServiceInstance")
	cache := prepare(t)
	assert.Nil(t, cache.Refresh())
	instances := make(map[string]bool)
	for i := 0; i < 4000; i++ {
		service, instanceErr := cache.GetServiceInstance("authentication-service")
		assert.Nil(t, instanceErr)
		instances[service.Address] = true
		if len(instances) > 2 {
			break
		}
	}
	assert.Equal(t, len(instances), 3)
	fmt.Println("TestGetServiceInstance done")
}

func TestGetServiceAddress(t *testing.T) {
	fmt.Println("TestGetServiceAddress")
	cache := prepare(t)
	assert.Nil(t, cache.Refresh())
	instances := make(map[string]bool)
	for i := 0; i < 4000; i++ {
		address, instanceErr := cache.GetServiceAddress("authentication-service")
		assert.Nil(t, instanceErr)
		instances[address] = true
		if len(instances) > 2 {
			break
		}
	}

	assert.Equal(t, len(instances), 3)
	for i := 1; i < 4; i++ {
		expectedAddress := fmt.Sprintf("192.168.2.%d:80", i)
		assert.True(t, instances[expectedAddress], "expected ", expectedAddress, " but could not find in result")
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
