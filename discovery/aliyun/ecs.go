package aliyun

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/denverdino/aliyungo/metadata"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	ecs_pop "github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
)

const (
	ecsLabel = model.MetaLabelPrefix + "ecs_"

	ecsLabelPublicIp  = ecsLabel + "public_ip"  // classic public ip
	ecsLabelInnerIp   = ecsLabel + "inner_ip"   // classic inner ip
	ecsLabelEip       = ecsLabel + "eip"        // vpc public eip
	ecsLabelPrivateIp = ecsLabel + "private_ip" // vpc private ip

	ecsLabelInstanceId  = ecsLabel + "instance_id"
	ecsLabelRegionId    = ecsLabel + "region_id"
	ecsLabelStatus      = ecsLabel + "status"
	ecsLabelZoneId      = ecsLabel + "zone_id"
	ecsLabelNetworkType = ecsLabel + "network_type"
	ecsLabelUserId      = ecsLabel + "user_id"
	ecsLabelTag         = ecsLabel + "tag_"

	MAX_PAGE_LIMIT = 50 // it's limited by ecs describeInstances API
)

var DefaultECSConfig = ECSConfig{
	Port:            8888,
	RefreshInterval: model.Duration(60 * time.Second),
	Limit:           100,
}

// ECSConfig is the configuration for Aliyun based service discovery.
type ECSConfig struct {
	Port            int            `yaml:"port"`
	UserId          string         `yaml:"user_id,omitempty"`
	RefreshInterval model.Duration `yaml:"refresh_interval,omitempty"`
	RegionId        string         `yaml:"region_id,omitempty"`
	TagFilters      []*TagFilter   `yaml:"tag_filters"`

	// Alibaba ECS Auth Args
	// https://github.com/aliyun/alibaba-cloud-sdk-go/blob/master/docs/2-Client-EN.md
	AccessKey         string `yaml:"access_key,omitempty"`
	AccessKeySecret   string `yaml:"access_key_secret,omitempty"`
	StsToken          string `yaml:"sts_token,omitempty"`
	RoleArn           string `yaml:"role_arn,omitempty"`
	RoleSessionName   string `yaml:"role_session_name,omitempty"`
	Policy            string `yaml:"policy,omitempty"`
	RoleName          string `yaml:"role_name,omitempty"`
	PublicKeyId       string `yaml:"public_key_id,omitempty"`
	PrivateKey        string `yaml:"private_key,omitempty"`
	SessionExpiration int    `yaml:"session_expiration,omitempty"`

	// query ecs limit, default is 100.
	Limit int `yaml:"limit,omitempty"`
}

func init() {
	discovery.RegisterConfig(&ECSConfig{})
}

// Name returns the name of the ECS Config.
func (*ECSConfig) Name() string { return "ecs" }

// NewDiscoverer returns a Discoverer for the ECS Config.
func (c *ECSConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return NewECSDiscovery(c, opts.Logger, opts.Metrics)
}

// NewDiscovererMetrics implements discovery.Config.
func (*ECSConfig) NewDiscovererMetrics(reg prometheus.Registerer, rmi discovery.RefreshMetricsInstantiator) discovery.DiscovererMetrics {
	return newDiscovererMetrics(reg, rmi)
}

func (c *ECSConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultECSConfig
	type plain ECSConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	for _, f := range c.TagFilters {
		if len(f.Values) == 0 {
			return errors.New("ECS SD configuration filter values cannot be empty")
		}
	}
	if len(c.RegionId) == 0 {
		return errors.New("ECS SD configuration need RegionId")
	}
	return nil
}

// Filter is the configuration tags for filtering ECS instances.
type TagFilter struct {
	Key    string   `yaml:"key"`
	Values []string `yaml:"values"`
}

type Discovery struct {
	*refresh.Discovery
	logger log.Logger
	ecsCfg *ECSConfig
	port   int
	limit  int

	tgCache *targetgroup.Group
	metrics *ecsMetrics
}

// NewECSDiscovery returns a new ECSDiscovery which periodically refreshes its targets.
func NewECSDiscovery(cfg *ECSConfig, logger log.Logger, metrics discovery.DiscovererMetrics) (discovery.Discoverer, error) {
	m, ok := metrics.(*ecsMetrics)
	if !ok {
		return nil, fmt.Errorf("invalid discovery metrics type")
	}

	if logger == nil {
		logger = log.NewNopLogger()
	}
	d := &Discovery{
		ecsCfg:  cfg,
		port:    cfg.Port,
		limit:   cfg.Limit,
		logger:  logger,
		tgCache: &targetgroup.Group{},
		metrics: m,
	}

	d.Discovery = refresh.NewDiscovery(refresh.Options{
		Logger:              logger,
		Mech:                "ecs",
		Interval:            time.Duration(cfg.RefreshInterval),
		RefreshF:            d.refresh,
		MetricsInstantiator: m.refreshMetrics,
	})
	return d, nil
}

func (d *Discovery) refresh(ctx context.Context) ([]*targetgroup.Group, error) {
	defer level.Debug(d.logger).Log("msg", "ECS discovery completed")

	d.metrics.queryCount.Inc()

	level.Info(d.logger).Log("msg", "New ECS Client with config and logger.")
	client, err := NewECSClient(d.ecsCfg, d.logger)
	if err != nil {
		level.Debug(d.logger).Log("msg", "NewECSClient", "err: ", err)
		return nil, err
	}

	level.Info(d.logger).Log("msg", "Query Instances with Aliyun OpenAPI.")
	instances, err := client.QueryInstances(d.ecsCfg.TagFilters, d.tgCache)
	if err != nil {
		level.Debug(d.logger).Log("msg", "QueryInstances", "err: ", err)
		d.metrics.queryFailuresCount.Inc()
		return nil, err
	}

	// build instances list.
	level.Info(d.logger).Log("msg", "Found Instances from remote during ECS discovery.", "count", len(instances))

	tg := &targetgroup.Group{
		Source: d.ecsCfg.RegionId,
	}

	noIpAddressInstanceCount := 0
	for _, instance := range instances {
		labels, err := addLabel(d.ecsCfg.UserId, d.port, instance)
		if err != nil {
			noIpAddressInstanceCount++
			level.Debug(d.logger).Log("msg", "Instance dont have AddressLabel.", "instance: ", fmt.Sprintf("%v", instance))
			continue
		}
		tg.Targets = append(tg.Targets, labels)
	}

	level.Info(d.logger).Log("msg", "Found Instances during ECS discovery.", "count", len(tg.Targets))
	if noIpAddressInstanceCount > 0 {
		level.Info(d.logger).Log("msg", "Found no AddressLabel instances during ECS discovery.", "count", noIpAddressInstanceCount)
	}

	// cache targetGroup
	d.tgCache = tg

	return []*targetgroup.Group{tg}, nil
}

func (cli *ecsClient) QueryInstances(tagFilters []*TagFilter, cache *targetgroup.Group) ([]ecs_pop.Instance, error) {
	if len(tagFilters) > 0 { // len(tagFilters) = 0 when tagFilters = nil
		// 1. tagFilter situation. query ListTagResources first, then query DescribeInstances
		instancesFromListTagResources, err := cli.queryFromListTagResources(tagFilters)
		if err != nil {
			level.Debug(cli.logger).Log("msg", "Query Instances from ListTagResources during ECS discovery.", "err", err)
			return nil, err
		}
		return instancesFromListTagResources, nil
	}

	// 2. no tagFilter situation. query DescribeInstances, then do cache double check.
	instancesFromDescribeInstances, err := cli.queryFromDescribeInstances()
	if err != nil {
		level.Debug(cli.logger).Log("msg", "Query Instances from DescribeInstances during ECS discovery.", "err", err)
		return nil, fmt.Errorf("query from DescribeInstances in QueryInstances, err: %w", err)
	}
	instancesFromCacheReCheck := cli.getCacheReCheckInstances(cache)
	level.Info(cli.logger).Log("msg", "Found Instances from cache re-check during ECS discovery.", "count", len(instancesFromCacheReCheck))
	instances := mergeHashInstances(instancesFromDescribeInstances, instancesFromCacheReCheck)
	return instances, nil
}

// listTagInstanceIds get instance ids and filterred by tag
func (cli *ecsClient) listTagInstanceIds(token string, tagFilters []*TagFilter) ([]string, string, error) {
	listTagResourcesRequest := ecs_pop.CreateListTagResourcesRequest()
	listTagResourcesRequest.RegionId = cli.regionId
	listTagResourcesRequest.ResourceType = "instance"

	// FIRST token is empty, and continue
	if token != "FIRST" {
		if token != "" && token != "ICM=" {
			listTagResourcesRequest.NextToken = token
		} else {
			return []string{}, "", errors.New("token is empty, but not first request")
		}
	}

	filters := tagFiltersCast(tagFilters)
	listTagResourcesRequest.TagFilter = &filters
	response, err := cli.ListTagResources(listTagResourcesRequest)
	if err != nil {
		return []string{}, "", fmt.Errorf("response from ListTagResources, err: %w", err)
	}
	level.Debug(cli.logger).Log("msg", "get response from ListTagResources.", "response: ", response)

	tagResources := response.TagResources.TagResource
	if len(tagResources) == 0 { // len(tagResources) = 0 when tagResources = nil
		level.Debug(cli.logger).Log("msg", "ListTagResourcesTagFilter found no resources.", "response: ", response)
		return []string{}, "", nil
	}

	var resourceIds []string
	for _, tagResource := range tagResources {
		resourceIds = append(resourceIds, tagResource.ResourceId)
	}
	level.Debug(cli.logger).Log("msg", "listTagResource and get ECS instanceIds. for ListTagResourcesTagFilter.", "instanceIds: ", resourceIds)
	return resourceIds, response.NextToken, nil
}

func tagFiltersCast(tagFilters []*TagFilter) (ret []ecs_pop.ListTagResourcesTagFilter) {
	for _, tagFilter := range tagFilters {
		if len(tagFilter.Values) == 0 {
			return
		}
		tagFilter := ecs_pop.ListTagResourcesTagFilter{
			TagKey:    tagFilter.Key,
			TagValues: &tagFilter.Values,
		}
		ret = append(ret, tagFilter)
	}
	return
}

func (cli *ecsClient) queryFromDescribeInstances() ([]ecs_pop.Instance, error) {
	describeInstancesRequest := ecs_pop.CreateDescribeInstancesRequest()
	describeInstancesRequest.RegionId = cli.regionId
	describeInstancesRequest.PageSize = requests.NewInteger(MAX_PAGE_LIMIT)

	instances := make([]ecs_pop.Instance, 0)
	pageLimit := MAX_PAGE_LIMIT // number of instances in one page
	pageNumber := 1
	neededCount := MAX_PAGE_LIMIT // number of instances still needed
	if cli.limit >= 0 {
		neededCount = cli.limit
	}

	for neededCount > 0 {
		describeInstancesRequest.PageNumber = requests.NewInteger(pageNumber)
		response, err := cli.DescribeInstances(describeInstancesRequest)
		if err != nil {
			return nil, fmt.Errorf("could not get ecs describeInstances response, err: %w", err)
		}

		count := len(response.Instances.Instance)
		if count == 0 {
			break
		}
		// first page
		if pageNumber == 1 {
			neededCount = response.TotalCount
			if cli.limit > 0 {
				neededCount = min(neededCount, cli.limit)
			}
		}
		// if current page is last page, neededCount < count
		// else neededCount >= count
		pageLimit = min(count, neededCount)
		instances = append(instances, response.Instances.Instance[:pageLimit]...)

		neededCount -= count
		pageNumber++
	}
	return instances, nil
}

// getCacheReCheckInstances
// get cache targetGroup's instanceIds, and query DescribeInstances again to double check.
// every 50 instance per page.
func (cli *ecsClient) getCacheReCheckInstances(cache *targetgroup.Group) (retInstances []ecs_pop.Instance) {
	pageCount := 0
	instanceIds := []string{}
	for tgLabelSetIndex, tgLabelSet := range cache.Targets {
		pageCount++

		instanceId := tgLabelSet[ecsLabelInstanceId]
		instanceIds = append(instanceIds, string(instanceId))

		// full of one page, or last one of LabelSet Series.
		if pageCount >= MAX_PAGE_LIMIT || tgLabelSetIndex == (len(cache.Targets)-1) {
			// query instances
			instances, err := cli.describeInstances(instanceIds)
			if err != nil {
				level.Error(cli.logger).Log("msg", "getCacheReCheckInstances describeInstancesResponse err.", "err: ", err)
				continue
			}

			retInstances = append(retInstances, instances...)
			// clean page
			pageCount = 0
			instanceIds = []string{}
		}
	}
	return retInstances
}

// queryFromListTagResources
// token query
func (cli *ecsClient) queryFromListTagResources(tagFilters []*TagFilter) (instances []ecs_pop.Instance, err error) {
	token := "FIRST"
	var currentInstances []ecs_pop.Instance
	currentTotalCount := 0 // the number of instances that have been queried
	originalToken := "INIT"
	for {
		if token == "" || token == "ICM=" || token == originalToken {
			break
		}

		originalToken = token
		token, currentInstances, err = cli.listTagInstances(token, currentTotalCount, tagFilters)
		if err != nil {
			return nil, fmt.Errorf("list tag instances, err: %w", err)
		}

		if len(currentInstances) == 0 {
			break
		}
		currentTotalCount += len(currentInstances)
		instances = append(instances, currentInstances...)
	}
	return instances, nil
}

// describeInstances get instance
// page query, max size 50 every page
func (cli *ecsClient) describeInstances(ids []string) ([]ecs_pop.Instance, error) {
	describeInstancesRequest := ecs_pop.CreateDescribeInstancesRequest()
	describeInstancesRequest.RegionId = cli.regionId
	idsJson, err := json.Marshal(ids)
	if err != nil {
		return []ecs_pop.Instance{}, err
	}
	describeInstancesRequest.InstanceIds = string(idsJson)
	describeInstancesRequest.PageNumber = requests.NewInteger(1)
	describeInstancesRequest.PageSize = requests.NewInteger(MAX_PAGE_LIMIT)

	response, err := cli.DescribeInstances(describeInstancesRequest)
	if err != nil {
		return []ecs_pop.Instance{}, fmt.Errorf("could not invoke DescribeInstances API, err: %w", err)
	}
	return response.Instances.Instance, nil
}

func (cli *ecsClient) listTagInstances(token string, currentTotalCount int, tagFilters []*TagFilter) (string, []ecs_pop.Instance, error) {
	ids, nextToken, err := cli.listTagInstanceIds(token, tagFilters)
	if err != nil {
		return "", nil, fmt.Errorf("get ecs instanceIds, err: %w", err)
	}
	instances, err := cli.describeInstances(ids)
	if err != nil {
		return "", nil, fmt.Errorf("get ecs instance ids err: %w", err)
	}

	pageLimit := len(instances)
	currentLimit := cli.limit - currentTotalCount // remaining instance count
	if 0 <= currentLimit && currentLimit < pageLimit {
		pageLimit = currentLimit
	}

	return nextToken, instances[:pageLimit], nil
}

type client interface {
	DescribeInstances(request *ecs_pop.DescribeInstancesRequest) (response *ecs_pop.DescribeInstancesResponse, err error)
	ListTagResources(request *ecs_pop.ListTagResourcesRequest) (response *ecs_pop.ListTagResourcesResponse, err error)
}

var _ client = &ecsClient{}

type ecsClient struct {
	regionId string
	limit    int
	client
	logger log.Logger
}

func NewECSClient(config *ECSConfig, logger log.Logger) (*ecsClient, error) {
	cli, err := getEcsClient(config, logger)
	if err != nil {
		return nil, err
	}
	return &ecsClient{
		regionId: config.RegionId,
		limit:    config.Limit,
		client:   cli,
		logger:   logger,
	}, nil
}

func getEcsClient(config *ECSConfig, logger log.Logger) (client *ecs_pop.Client, err error) {
	level.Debug(logger).Log("msg", "Start to get Ecs Client.")

	if config.RegionId == "" {
		return nil, errors.New("Aliyun ECS service discovery config need regionId.")
	}

	// 1. Args

	// NewClientWithRamRoleArnAndPolicy
	if config.Policy != "" && config.AccessKey != "" && config.AccessKeySecret != "" && config.RoleArn != "" && config.RoleSessionName != "" {
		client, clientErr := ecs_pop.NewClientWithRamRoleArnAndPolicy(config.RegionId, config.AccessKey, config.AccessKeySecret, config.RoleArn, config.RoleSessionName, config.Policy)
		return client, clientErr
	}

	// NewClientWithRamRoleArn
	if config.RoleSessionName != "" && config.AccessKey != "" && config.AccessKeySecret != "" && config.RoleArn != "" {
		client, clientErr := ecs_pop.NewClientWithRamRoleArn(config.RegionId, config.AccessKey, config.AccessKeySecret, config.RoleArn, config.RoleSessionName)
		return client, clientErr
	}

	// NewClientWithStsToken
	if config.StsToken != "" && config.AccessKey != "" && config.AccessKeySecret != "" {
		client, clientErr := ecs_pop.NewClientWithStsToken(config.RegionId, config.AccessKey, config.AccessKeySecret, config.StsToken)
		return client, clientErr
	}

	// NewClientWithAccessKey
	if config.AccessKey != "" && config.AccessKeySecret != "" {
		client, clientErr := ecs_pop.NewClientWithAccessKey(config.RegionId, config.AccessKey, config.AccessKeySecret)
		return client, clientErr
	}

	// NewClientWithEcsRamRole
	if config.RoleName != "" {
		client, clientErr := ecs_pop.NewClientWithEcsRamRole(config.RegionId, config.RoleName)
		return client, clientErr
	}

	// NewClientWithRsaKeyPair
	if config.PublicKeyId != "" && config.PrivateKey != "" && config.SessionExpiration != 0 {
		client, clientErr := ecs_pop.NewClientWithRsaKeyPair(config.RegionId, config.PublicKeyId, config.PrivateKey, config.SessionExpiration)
		return client, clientErr
	}

	level.Debug(logger).Log("msg", "Start to get Ecs Client from ram.")

	// 2. ACS
	//get all RoleName for check

	metaData := metadata.NewMetaData(nil)
	var allRoleName metadata.ResultList
	allRoleNameErr := metaData.New().Resource("ram/security-credentials/").Do(&allRoleName)
	if allRoleNameErr != nil {
		level.Error(logger).Log("msg", "Get ECS Client from ram allRoleNameErr.", "err: ", allRoleNameErr)
		return nil, errors.New("Aliyun ECS service discovery cant init client, need auth config.")
	} else {
		roleName, roleNameErr := metaData.RoleName()

		level.Debug(logger).Log("msg", "Start to get Ecs Client from ram2.")

		if roleNameErr != nil {
			level.Error(logger).Log("msg", "Get ECS Client from ram roleNameErr.", "err: ", roleNameErr)
			return nil, errors.New("Aliyun ECS service discovery cant init client, need auth config.")
		} else {
			roleAuth, roleAuthErr := metaData.RamRoleToken(roleName)

			level.Debug(logger).Log("msg", "Start to get Ecs Client from ram3.")

			if roleAuthErr != nil {
				level.Error(logger).Log("msg", "Get ECS Client from ram roleAuthErr.", "err: ", roleAuthErr)
				return nil, errors.New("Aliyun ECS service discovery cant init client, need auth config.")
			} else {
				client := ecs_pop.Client{}
				clientConfig := client.InitClientConfig()
				clientConfig.Debug = true
				clientErr := client.InitWithStsToken(config.RegionId, roleAuth.AccessKeyId, roleAuth.AccessKeySecret, roleAuth.SecurityToken)

				level.Debug(logger).Log("msg", "Start to get Ecs Client from ram4.")

				if clientErr != nil {
					level.Error(logger).Log("msg", "Get ECS Client from ram clientErr.", "err: ", clientErr)
					return nil, errors.New("Aliyun ECS service discovery cant init client, need auth config.")
				} else {
					return &client, nil
				}
			}
		}
	}
	return nil, errors.New("Aliyun ECS service discovery cant init client, need auth config.")
}

// mergeHashInstances hash by instanceId and merge. O(n + m)
// The purpose is to remove duplicate elements
func mergeHashInstances(instances1 []ecs_pop.Instance, instances2 []ecs_pop.Instance) []ecs_pop.Instance {
	instanceId_instance := make(map[string]ecs_pop.Instance)
	for _, each := range instances1 {
		instanceId_instance[each.InstanceId] = each
	}
	for _, each := range instances2 {
		instanceId_instance[each.InstanceId] = each
	}

	retInstanceList := []ecs_pop.Instance{}
	for _, eachInstance := range instanceId_instance {
		retInstanceList = append(retInstanceList, eachInstance)
	}
	return retInstanceList
}

// addLabel add label, return LabelSet and error (!=nil when isAddressLabelExist equal to false)
func addLabel(userId string, port int, instance ecs_pop.Instance) (model.LabelSet, error) {
	labels := model.LabelSet{
		ecsLabelInstanceId:  model.LabelValue(instance.InstanceId),
		ecsLabelRegionId:    model.LabelValue(instance.RegionId),
		ecsLabelStatus:      model.LabelValue(instance.Status),
		ecsLabelZoneId:      model.LabelValue(instance.ZoneId),
		ecsLabelNetworkType: model.LabelValue(instance.InstanceNetworkType),
	}

	if userId != "" {
		labels[ecsLabelUserId] = model.LabelValue(userId)
	}

	// instance must have AddressLabel
	isAddressLabelExist := false

	// check classic public ip
	if len(instance.PublicIpAddress.IpAddress) > 0 {
		labels[ecsLabelPublicIp] = model.LabelValue(instance.PublicIpAddress.IpAddress[0])
		addr := net.JoinHostPort(instance.PublicIpAddress.IpAddress[0], fmt.Sprintf("%d", port))
		labels[model.AddressLabel] = model.LabelValue(addr)
		isAddressLabelExist = true
	}

	// check classic inner ip
	if len(instance.InnerIpAddress.IpAddress) > 0 {
		labels[ecsLabelInnerIp] = model.LabelValue(instance.InnerIpAddress.IpAddress[0])
		addr := net.JoinHostPort(instance.InnerIpAddress.IpAddress[0], fmt.Sprintf("%d", port))
		labels[model.AddressLabel] = model.LabelValue(addr)
		isAddressLabelExist = true
	}

	// check vpc eip
	if instance.EipAddress.IpAddress != "" {
		labels[ecsLabelEip] = model.LabelValue(instance.EipAddress.IpAddress)
		addr := net.JoinHostPort(instance.EipAddress.IpAddress, fmt.Sprintf("%d", port))
		labels[model.AddressLabel] = model.LabelValue(addr)
		isAddressLabelExist = true
	}

	// check vpc private ip
	if len(instance.VpcAttributes.PrivateIpAddress.IpAddress) > 0 {
		labels[ecsLabelPrivateIp] = model.LabelValue(instance.VpcAttributes.PrivateIpAddress.IpAddress[0])
		addr := net.JoinHostPort(instance.VpcAttributes.PrivateIpAddress.IpAddress[0], fmt.Sprintf("%d", port))
		labels[model.AddressLabel] = model.LabelValue(addr)
		isAddressLabelExist = true
	}

	if !isAddressLabelExist {
		return nil, errors.New(fmt.Sprintf("Instance %s dont have AddressLabel.", instance.InstanceId))
	}

	// tags
	for _, tag := range instance.Tags.Tag {
		labels[ecsLabelTag+model.LabelName(tag.TagKey)] = model.LabelValue(tag.TagValue)
	}
	return labels, nil
}
