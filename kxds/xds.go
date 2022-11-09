package kxds

import (
	"errors"
	"fmt"
	"strings"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	router "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	matcher "github.com/envoyproxy/go-control-plane/envoy/type/matcher/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	kcorev1 "k8s.io/api/core/v1"
	kmetav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"

	kxdsv1alpha1 "github.com/jlevesy/kxds/api/v1alpha1"
)

type xdsService struct {
	listener        types.Resource
	routeConfig     types.Resource
	clusters        []types.Resource
	loadAssignments []types.Resource
}

func makeXDSService(svc kxdsv1alpha1.XDSService, k8sEndpoints map[ktypes.NamespacedName]kcorev1.Endpoints) (xdsService, error) {
	var (
		err error

		resourcePrefix  = "kxds" + "." + svc.Name + "." + svc.Namespace + "."
		listenerName    = svc.Spec.Listener
		routeConfigName = resourcePrefix + "routeconfig"

		xdsSvc = xdsService{
			listener: makeListener(svc, routeConfigName),
			clusters: make([]types.Resource, len(svc.Spec.Clusters)),
		}
	)

	xdsSvc.routeConfig, err = makeRouteConfig(resourcePrefix, routeConfigName, listenerName, svc.Spec.Routes)
	if err != nil {
		return xdsSvc, err
	}

	for i, clusterSpec := range svc.Spec.Clusters {
		clusterName := resourcePrefix + clusterSpec.Name

		xdsSvc.clusters[i] = makeCluster(clusterName)

		loadAssignment, err := makeLoadAssignment(
			clusterName,
			svc.Namespace,
			clusterSpec.Localities,
			k8sEndpoints,
		)
		if err != nil {
			return xdsSvc, err
		}

		xdsSvc.loadAssignments = append(xdsSvc.loadAssignments, loadAssignment)
	}

	return xdsSvc, nil
}

func makeListener(svc kxdsv1alpha1.XDSService, routeConfigName string) *listener.Listener {
	httpConnManager := &hcm.HttpConnectionManager{
		CommonHttpProtocolOptions: &core.HttpProtocolOptions{
			MaxStreamDuration: makeDuration(svc.Spec.MaxStreamDuration),
		},
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{
			Rds: &hcm.Rds{
				RouteConfigName: routeConfigName,
				ConfigSource: &core.ConfigSource{
					ResourceApiVersion:    core.ApiVersion_V3,
					ConfigSourceSpecifier: &core.ConfigSource_Ads{Ads: &core.AggregatedConfigSource{}},
				},
			},
		},
		HttpFilters: []*hcm.HttpFilter{
			{
				Name: wellknown.Router,
				ConfigType: &hcm.HttpFilter_TypedConfig{
					TypedConfig: mustAny(&router.Router{}),
				},
			},
		},
	}

	return &listener.Listener{
		Name: svc.Spec.Listener,
		ApiListener: &listener.ApiListener{
			ApiListener: mustAny(httpConnManager),
		},
	}
}

func makeDuration(duration *kmetav1.Duration) *durationpb.Duration {
	if duration == nil {
		return nil
	}

	return durationpb.New(duration.Duration)
}

func makeRouteConfig(resourcePrefix, routeConfigName, listenerName string, routeSpecs []kxdsv1alpha1.Route) (*route.RouteConfiguration, error) {
	routes := make([]*route.Route, len(routeSpecs))

	for i, routeSpec := range routeSpecs {
		match, err := makeRouteMatch(routeSpec)
		if err != nil {
			return nil, err
		}

		routes[i] = &route.Route{
			Match: match,
			Action: &route.Route_Route{
				Route: &route.RouteAction{
					MaxStreamDuration: &route.RouteAction_MaxStreamDuration{
						MaxStreamDuration:    makeDuration(routeSpec.MaxStreamDuration),
						GrpcTimeoutHeaderMax: makeDuration(routeSpec.GrpcTimeoutHeaderMax),
					},
					ClusterSpecifier: &route.RouteAction_WeightedClusters{
						WeightedClusters: makeWeightedClusters(resourcePrefix, routeSpec),
					},
				},
			},
		}
	}

	return &route.RouteConfiguration{
		Name:             routeConfigName,
		ValidateClusters: &wrapperspb.BoolValue{Value: true},
		VirtualHosts: []*route.VirtualHost{
			{
				Name:    resourcePrefix + "vhost",
				Domains: []string{listenerName},
				Routes:  routes,
			},
		},
	}, nil
}

func makeRouteMatch(spec kxdsv1alpha1.Route) (*route.RouteMatch, error) {
	var match route.RouteMatch

	switch {
	case spec.Path.Regex != nil:
		regexMatcher, err := makeRegexMatcher(spec.Path.Regex)
		if err != nil {
			return nil, err
		}

		match.PathSpecifier = &route.RouteMatch_SafeRegex{
			SafeRegex: regexMatcher,
		}

	case spec.Path.Path != "":
		match.PathSpecifier = &route.RouteMatch_Path{
			Path: spec.Path.Path,
		}
	default:
		match.PathSpecifier = &route.RouteMatch_Prefix{
			Prefix: spec.Path.Prefix,
		}
	}

	match.CaseSensitive = wrapperspb.Bool(spec.CaseSensitive)
	match.Headers = make([]*route.HeaderMatcher, len(spec.Headers))

	if spec.RuntimeFraction != nil {
		denominator, ok := typev3.FractionalPercent_DenominatorType_value[strings.ToUpper(spec.RuntimeFraction.Denominator)]
		if !ok {
			return nil, fmt.Errorf(
				"unsupported denominator %q for runtime fraction",
				spec.RuntimeFraction.Denominator,
			)
		}

		match.RuntimeFraction = &core.RuntimeFractionalPercent{
			DefaultValue: &typev3.FractionalPercent{
				Numerator:   spec.RuntimeFraction.Numerator,
				Denominator: typev3.FractionalPercent_DenominatorType(denominator),
			},
		}
	}

	for i, headerMatcherSpec := range spec.Headers {
		var err error

		match.Headers[i], err = makeHeaderMatcher(headerMatcherSpec)
		if err != nil {
			return nil, err
		}
	}

	return &match, nil
}

func makeHeaderMatcher(spec kxdsv1alpha1.HeaderMatcher) (*route.HeaderMatcher, error) {
	matcher := route.HeaderMatcher{
		Name:        spec.Name,
		InvertMatch: spec.Invert,
	}

	switch {
	case spec.Exact != nil:
		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_ExactMatch{
			ExactMatch: *spec.Exact,
		}
	case spec.Regex != nil:
		regexMatcher, err := makeRegexMatcher(spec.Regex)
		if err != nil {
			return nil, err
		}

		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_SafeRegexMatch{
			SafeRegexMatch: regexMatcher,
		}
	case spec.Range != nil:
		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_RangeMatch{
			RangeMatch: &typev3.Int64Range{
				Start: spec.Range.Start,
				End:   spec.Range.End,
			},
		}
	case spec.Present != nil:
		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_PresentMatch{
			PresentMatch: *spec.Present,
		}
	case spec.Prefix != nil:
		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_PrefixMatch{
			PrefixMatch: *spec.Prefix,
		}
	case spec.Suffix != nil:
		matcher.HeaderMatchSpecifier = &route.HeaderMatcher_SuffixMatch{
			SuffixMatch: *spec.Suffix,
		}
	default:
		return nil, errors.New("invalid header matcher")

	}

	return &matcher, nil
}

func makeRegexMatcher(spec *kxdsv1alpha1.RegexMatcher) (*matcher.RegexMatcher, error) {
	if spec.Engine != "re2" {
		return nil, fmt.Errorf("unsupported engine %q", spec.Engine)
	}

	if spec.Regex == "" {
		return nil, errors.New("blank regex")
	}

	return &matcher.RegexMatcher{
		Regex: spec.Regex,
		EngineType: &matcher.RegexMatcher_GoogleRe2{
			GoogleRe2: &matcher.RegexMatcher_GoogleRE2{},
		},
	}, nil
}

func makeWeightedClusters(resourcePrefix string, routeSpec kxdsv1alpha1.Route) *route.WeightedCluster {
	var (
		totalWeight     uint32
		weighedClusters = make([]*route.WeightedCluster_ClusterWeight, len(routeSpec.Clusters))
	)

	for i, clusterRef := range routeSpec.Clusters {
		totalWeight += clusterRef.Weight
		weighedClusters[i] = &route.WeightedCluster_ClusterWeight{
			Name:   resourcePrefix + clusterRef.Name,
			Weight: wrapperspb.UInt32(clusterRef.Weight),
		}
	}

	return &route.WeightedCluster{
		TotalWeight: wrapperspb.UInt32(totalWeight),
		Clusters:    weighedClusters,
	}
}

func makeCluster(clusterName string) *cluster.Cluster {
	return &cluster.Cluster{
		Name:                 clusterName,
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_EDS},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: &core.ConfigSource{
				ConfigSourceSpecifier: &core.ConfigSource_Ads{
					Ads: &core.AggregatedConfigSource{},
				},
			},
			ServiceName: clusterName,
		},
		LbPolicy: cluster.Cluster_ROUND_ROBIN,
	}
}

func makeLoadAssignment(clusterName, currentNamespace string, localities []kxdsv1alpha1.Locality, k8sEndpoints map[ktypes.NamespacedName]kcorev1.Endpoints) (*endpoint.ClusterLoadAssignment, error) {
	xdsLocalities := make([]*endpoint.LocalityLbEndpoints, len(localities))

	for i, locSpec := range localities {
		if locSpec.Service == nil {
			return nil, errors.New("unsupported non k8s service locality")
		}

		targetNamespace := locSpec.Service.Namespace

		if targetNamespace == "" {
			targetNamespace = currentNamespace
		}

		k8sEndpoint, ok := k8sEndpoints[ktypes.NamespacedName{Namespace: targetNamespace, Name: locSpec.Service.Name}]
		if !ok {
			return nil, errors.New("no k8s endpoints found")
		}

		var err error

		xdsLocalities[i], err = makeK8sLocality(locSpec, k8sEndpoint)
		if err != nil {
			return nil, fmt.Errorf("could not build cluster %q: %w", clusterName, err)
		}

	}

	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints:   xdsLocalities,
	}, nil
}

func makeK8sLocality(locSpec kxdsv1alpha1.Locality, k8sEndpoint kcorev1.Endpoints) (*endpoint.LocalityLbEndpoints, error) {
	var xdsEndpoints []*endpoint.LbEndpoint

	for _, ep := range k8sEndpoint.Subsets {
		port, ok := lookupK8sPort(locSpec.Service.Port, ep)
		if !ok {
			return nil, errors.New("no desired port found on the k8s endpoint")
		}

		xdsEndpoints = append(
			xdsEndpoints,
			makeEndpointsFromSubset(ep, port)...,
		)
	}

	return &endpoint.LocalityLbEndpoints{
		Locality:            &core.Locality{SubZone: locSpec.Service.Name},
		LoadBalancingWeight: wrapperspb.UInt32(locSpec.Weight),
		Priority:            locSpec.Priority,
		LbEndpoints:         xdsEndpoints,
	}, nil

}

func makeEndpointsFromSubset(ep kcorev1.EndpointSubset, port uint32) []*endpoint.LbEndpoint {
	eps := make([]*endpoint.LbEndpoint, len(ep.Addresses))

	for i, ep := range ep.Addresses {
		eps[i] = &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: &core.Address{
						Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Protocol: core.SocketAddress_TCP,
								Address:  ep.IP,
								PortSpecifier: &core.SocketAddress_PortValue{
									PortValue: port,
								},
							},
						},
					},
				},
			},
		}

	}

	return eps
}

func lookupK8sPort(k8sSvc kxdsv1alpha1.K8sPort, eps kcorev1.EndpointSubset) (uint32, bool) {
	if k8sSvc.Name != "" {
		for _, p := range eps.Ports {
			if p.Name == k8sSvc.Name {
				return uint32(p.Port), true
			}

		}

		return 0, false
	}

	for _, p := range eps.Ports {
		if p.Port == k8sSvc.Number {
			return uint32(p.Port), true
		}
	}

	return 0, false
}

func mustAny(msg protoreflect.ProtoMessage) *anypb.Any {
	p, err := anypb.New(msg)
	if err != nil {
		panic(err)
	}

	return p
}
