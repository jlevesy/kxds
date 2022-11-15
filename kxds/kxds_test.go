package kxds_test

import (
	"context"
	"testing"
	"time"

	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/stretchr/testify/require"
	_ "google.golang.org/grpc/xds"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kxdsv1alpha1 "github.com/jlevesy/kxds/api/v1alpha1"
	"github.com/jlevesy/kxds/kxds"
	"github.com/jlevesy/kxds/pkg/testruntime"
)

var (
	grpcPort = kxdsv1alpha1.K8sPort{
		Name: "grpc",
	}

	v1v2ClusterTopology = testruntime.WithClusters(
		testruntime.BuildCluster(
			"v2",
			testruntime.WithLocalities(
				testruntime.BuildLocality(
					testruntime.WithK8sService(
						kxdsv1alpha1.K8sService{
							Name: "test-service-v2",
							Port: grpcPort,
						},
					),
				),
			),
		),
		testruntime.BuildCluster(
			"v1",
			testruntime.WithLocalities(
				testruntime.BuildLocality(
					testruntime.WithK8sService(
						kxdsv1alpha1.K8sService{
							Name: "test-service",
							Port: grpcPort,
						},
					),
				),
			),
		),
	)
)

func TestReconciller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backends, err := testruntime.StartBackends(
		testruntime.Config{
			BackendCount: 10,
		},
	)
	require.NoError(t, err)
	defer func() {
		_ = backends.Stop()
	}()

	var (
		xdsCache = cache.NewSnapshotCache(
			false,
			kxds.DefaultHash,
			testruntime.NoopCacheLogger{},
		)

		server = kxds.NewXDSServer(
			xdsCache,
			kxds.XDSServerConfig{BindAddr: ":18000"},
		)
	)

	go func() {
		err := server.Start(ctx)
		require.NoError(t, err)
	}()

	for _, testCase := range []struct {
		desc             string
		endpoints        []corev1.Endpoints
		xdsServices      []kxdsv1alpha1.XDSService
		backendsBehavior func(t *testing.T, bs testruntime.Backends)
		doAssert         func(t *testing.T)
	}{
		{
			desc: "single call port by name",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.AssertAggregatedValue("backend-0", 1),
				),
			),
		},
		{
			desc: "single call port by number",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: kxdsv1alpha1.K8sPort{
												Number: backends[0].PortNumber(),
											},
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.AssertAggregatedValue("backend-0", 1),
				),
			),
		},
		{
			desc: "cross namespace",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "some-app", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name:      "test-service",
											Namespace: "some-app",
											Port:      grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.AssertAggregatedValue("backend-0", 1),
				),
			),
		},
		{
			desc: "locality based wrr",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:2]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[2:4]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithLocalityWeight(80),
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
								testruntime.BuildLocality(
									testruntime.WithLocalityWeight(20),
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service-v2",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallN(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				10000,
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					// 80% of calls
					testruntime.AssertAggregatedValueWithinDelta("backend-0", 4000, 500.0),
					testruntime.AssertAggregatedValueWithinDelta("backend-1", 4000, 500.0),
					// 20% of calls
					testruntime.AssertAggregatedValueWithinDelta("backend-2", 1000, 500.0),
					testruntime.AssertAggregatedValueWithinDelta("backend-3", 1000, 500.0),
				),
			),
		},
		{
			desc: "priority fallback",
			endpoints: []corev1.Endpoints{
				// No backends for the test-service in that case.
				testruntime.BuildEndpoints("test-service", "default", backends[0:0]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithLocalityPriority(0),
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
								testruntime.BuildLocality(
									testruntime.WithLocalityPriority(1),
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service-v2",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					// No calls for the first set of backends
					testruntime.AssertAggregatedValue("backend-0", 0),
					// One call for the second backend.
					testruntime.AssertAggregatedValue("backend-1", 1),
				),
			),
		},
		{
			desc: "exact path matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithPathMatcher(
								kxdsv1alpha1.PathMatcher{
									Path: "/echo.Echo/EchoPremium",
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "prefix path matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithPathMatcher(
								kxdsv1alpha1.PathMatcher{
									Prefix: "/echo.Echo/EchoP",
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "regexp path matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithPathMatcher(
								kxdsv1alpha1.PathMatcher{
									Prefix: "/echo.Echo/EchoP",
									Regex: &kxdsv1alpha1.RegexMatcher{
										Regex:  ".*/EchoPremium",
										Engine: "re2",
									},
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "case insensitive path matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithCaseSensitive(false),
							testruntime.WithPathMatcher(
								kxdsv1alpha1.PathMatcher{
									Prefix: "/echo.echo/echop",
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header invert matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								testruntime.HeaderInvertMatch(
									testruntime.HeaderExactMatch(
										"x-variant",
										"Awesome",
									),
								),
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								"x-variant": "Awesome",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-1", 0),
						testruntime.AssertAggregatedValue("backend-0", 1),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								"x-variant": "NotAwesome",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-0", 0),
						testruntime.AssertAggregatedValue("backend-1", 1),
					),
				),
			),
		},
		{
			desc: "header exact matching",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								testruntime.HeaderExactMatch(
									"x-variant",
									"Awesome",
								),
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Gotha, metadata keys are lowercased.
								"x-variant": "Awesome",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header safe regex match",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								kxdsv1alpha1.HeaderMatcher{
									Name: "x-variant",
									Regex: &kxdsv1alpha1.RegexMatcher{
										Regex:  "Awe.*",
										Engine: "re2",
									},
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								"x-variant": "Awesome",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header range match",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								kxdsv1alpha1.HeaderMatcher{
									Name: "x-variant",
									Range: &kxdsv1alpha1.RangeMatcher{
										Start: 10,
										End:   20,
									},
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// In range, call backend 1.
								"x-variant": "12",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Out of bound, call backend-0.
								"x-variant": "9",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header present match",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								testruntime.HeaderPresentMatch(
									"x-variant",
									true,
								),
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Header is present, send to v2.
								"x-variant": "wooop",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(testruntime.MethodEcho),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header prefix match",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								testruntime.HeaderPrefixMatch(
									"x-variant",
									"wo",
								),
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Header has the prefix wo, send to v2.
								"x-variant": "wooop",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Header has not the prefix wo, send to v1.
								"x-variant": "not",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "header suffix match",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithHeaderMatchers(
								testruntime.HeaderSuffixMatch(
									"x-variant",
									"oop",
								),
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Header has the sufix oop, send to v2.
								"x-variant": "wooop",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-1", 1),
						testruntime.AssertAggregatedValue("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Header has not the suffix oop, send to v1.
								"x-variant": "not",
							},
						),
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.AssertAggregatedValue("backend-0", 1),
						testruntime.AssertAggregatedValue("backend-1", 0),
					),
				),
			),
		},
		{
			desc: "runtime fraction traffic splitting",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
				testruntime.BuildEndpoints("test-service-v2", "default", backends[1:2]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							// 20.00% of the traffic will go to v2.
							testruntime.WithRuntimeFraction(
								kxdsv1alpha1.Fraction{
									Numerator:   20,
									Denominator: "hundred",
								},
							),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "v2",
									Weight: 1,
								},
							),
						),
						testruntime.BuildSingleRoute("v1"),
					),
					v1v2ClusterTopology,
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallN(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				10000,
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.AssertAggregatedValueWithinDelta("backend-1", 2000, 500.0),
					testruntime.AssertAggregatedValueWithinDelta("backend-0", 8000, 500.0),
				),
			),
		},
		{
			desc: "max stream duration",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(testruntime.BuildSingleRoute("default")),
					testruntime.WithMaxStreamDuration(50*time.Millisecond),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: hang(10 * time.Second),
			doAssert: testruntime.WithinDelay(
				time.Second,
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.MustFail,
				),
			),
		},
		{
			desc: "max stream duration on route",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildRoute(
							testruntime.WithRouteMaxStreamDuration(50*time.Millisecond),
							testruntime.WithClusterRefs(
								kxdsv1alpha1.ClusterRef{
									Name:   "default",
									Weight: 1,
								},
							),
						),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: hang(10 * time.Second),
			// TODO(jly): this is weird, this test takes  10s when running after max_stream_duration
			// but only 1s  when being run standalone. Something here is fishy.
			doAssert: testruntime.WithinDelay(
				time.Second,
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.MustFail,
				),
			),
		},
		{
			desc: "max requests on cluster",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithMaxRequests(1),
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: hang(1 * time.Second),
			doAssert: testruntime.CallNParallel(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				10,
				testruntime.AggregateByError(
					testruntime.AssertAggregatedValue("ok", 1),
					testruntime.AssertAggregatedValue(
						"rpc error: code = Unavailable desc = max requests 1 exceeded on service kxds.test-xds.default.default",
						9,
					),
				),
			),
		},
		{
			desc: "fixed delay injection",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithFilters(
						kxdsv1alpha1.Filter{
							Name: "test-filter",
							FilterConfig: kxdsv1alpha1.FilterConfig{
								Fault: &kxdsv1alpha1.FaultFilter{
									Delay: &kxdsv1alpha1.FaultDelay{
										Fixed: testruntime.DurationPtr(500 * time.Millisecond),
										Percentage: &kxdsv1alpha1.Fraction{
											Numerator:   100,
											Denominator: "hundred",
										},
									},
								},
							},
						},
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.ExceedDelay(
				200*time.Millisecond,
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
				),
			),
		},
		{
			desc: "header delay injection",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithFilters(
						kxdsv1alpha1.Filter{
							Name: "test-filter",
							FilterConfig: kxdsv1alpha1.FilterConfig{
								Fault: &kxdsv1alpha1.FaultFilter{
									Delay: &kxdsv1alpha1.FaultDelay{
										Header: &kxdsv1alpha1.HeaderFault{},
										Percentage: &kxdsv1alpha1.Fraction{
											Numerator:   100,
											Denominator: "hundred",
										},
									},
								},
							},
						},
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.ExceedDelay(
				200*time.Millisecond,
				testruntime.CallOnce(
					"xds:///default/test-xds",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
						testruntime.WithMetadata(
							map[string]string{
								// Delay by 500ms.
								"x-envoy-fault-delay-request": "500",
							},
						),
					),
					testruntime.NoCallErrors,
				),
			),
		},
		{
			desc: "abort injection http",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",

					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithFilters(
						kxdsv1alpha1.Filter{
							Name: "test-filter",
							FilterConfig: kxdsv1alpha1.FilterConfig{
								Fault: &kxdsv1alpha1.FaultFilter{
									Abort: &kxdsv1alpha1.FaultAbort{
										HTTPStatus: testruntime.Ptr(uint32(404)),
										Percentage: &kxdsv1alpha1.Fraction{
											Numerator:   100,
											Denominator: "hundred",
										},
									},
								},
							},
						},
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.MustFail,
			),
		},
		{
			desc: "abort injection grpc",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithFilters(
						kxdsv1alpha1.Filter{
							Name: "test-filter",
							FilterConfig: kxdsv1alpha1.FilterConfig{
								Fault: &kxdsv1alpha1.FaultFilter{
									Abort: &kxdsv1alpha1.FaultAbort{
										GRPCStatus: testruntime.Ptr(uint32(4)),
										Percentage: &kxdsv1alpha1.Fraction{
											Numerator:   100,
											Denominator: "hundred",
										},
									},
								},
							},
						},
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.MustFail,
			),
		},
		{
			desc: "abort header grpc",
			endpoints: []corev1.Endpoints{
				testruntime.BuildEndpoints("test-service", "default", backends[0:1]),
			},
			xdsServices: []kxdsv1alpha1.XDSService{
				testruntime.BuildXDSService(
					"test-xds",
					"default",
					testruntime.WithRoutes(
						testruntime.BuildSingleRoute("default"),
					),
					testruntime.WithFilters(
						kxdsv1alpha1.Filter{
							Name: "test-filter",
							FilterConfig: kxdsv1alpha1.FilterConfig{
								Fault: &kxdsv1alpha1.FaultFilter{
									Abort: &kxdsv1alpha1.FaultAbort{
										Header: &kxdsv1alpha1.HeaderFault{},
										Percentage: &kxdsv1alpha1.Fraction{
											Numerator:   100,
											Denominator: "hundred",
										},
									},
								},
							},
						},
					),
					testruntime.WithClusters(
						testruntime.BuildCluster(
							"default",
							testruntime.WithLocalities(
								testruntime.BuildLocality(
									testruntime.WithK8sService(
										kxdsv1alpha1.K8sService{
											Name: "test-service",
											Port: grpcPort,
										},
									),
								),
							),
						),
					),
				),
			},
			backendsBehavior: answer,
			doAssert: testruntime.CallOnce(
				"xds:///default/test-xds",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
					testruntime.WithMetadata(
						map[string]string{
							"x-envoy-fault-abort-grpc-request": "3",
						},
					),
				),
				testruntime.MustFail,
			),
		},
	} {
		t.Run(testCase.desc, func(t *testing.T) {
			var (
				cl = fake.NewClientBuilder().WithLists(
					&kxdsv1alpha1.XDSServiceList{Items: testCase.xdsServices},
					&corev1.EndpointsList{Items: testCase.endpoints},
				).Build()

				cacheReconciller = kxds.NewReconciler(
					cl,
					kxds.NewCacheRefresher(
						xdsCache,
						kxds.DefautHashKey,
					),
				)
			)

			testCase.backendsBehavior(t, backends)

			// Flush snapshot state from previous iteration.
			xdsCache.ClearSnapshot(kxds.DefautHashKey)

			_, err := cacheReconciller.Reconcile(
				ctx,
				ctrl.Request{},
			)
			require.NoError(t, err)

			testCase.doAssert(t)
		})
	}
}

func answer(t *testing.T, backends testruntime.Backends) {
	backends.SetBehavior(testruntime.DefaultBehavior())
}

func hang(d time.Duration) func(t *testing.T, backends testruntime.Backends) {
	return func(t *testing.T, backends testruntime.Backends) {
		backends.SetBehavior(testruntime.HangBehavior(d))
	}
}
