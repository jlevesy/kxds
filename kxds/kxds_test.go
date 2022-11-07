package kxds_test

import (
	"context"
	"testing"

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
		desc        string
		endpoints   []corev1.Endpoints
		xdsServices []kxdsv1alpha1.XDSService

		doAssert func(t *testing.T)
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
					"echo_server",
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
			doAssert: testruntime.CallOnce(
				"xds:///echo_server",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.BackendCalledExact("backend-0", 1),
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
					"echo_server",
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
			doAssert: testruntime.CallOnce(
				"xds:///echo_server",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.BackendCalledExact("backend-0", 1),
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
					"echo_server",
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
			doAssert: testruntime.CallOnce(
				"xds:///echo_server",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					testruntime.BackendCalledExact("backend-0", 1),
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
					"echo_server",
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
			doAssert: testruntime.CallN(
				"xds:///echo_server",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				10000,
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					// 80% of calls
					testruntime.BackendCalledDelta("backend-0", 4000, 500.0),
					testruntime.BackendCalledDelta("backend-1", 4000, 500.0),
					// 20% of calls
					testruntime.BackendCalledDelta("backend-2", 1000, 500.0),
					testruntime.BackendCalledDelta("backend-3", 1000, 500.0),
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
					"echo_server",
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
			doAssert: testruntime.CallOnce(
				"xds:///echo_server",
				testruntime.BuildCaller(
					testruntime.MethodEcho,
				),
				testruntime.NoCallErrors,
				testruntime.AggregateByBackendID(
					// No calls for the first set of backends
					testruntime.BackendCalledExact("backend-0", 0),
					// One call for the second backend.
					testruntime.BackendCalledExact("backend-1", 1),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEchoPremium,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// One call for the second backend, because we're calling premium.
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 0),
						testruntime.BackendCalledExact("backend-0", 1),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-0", 0),
						testruntime.BackendCalledExact("backend-1", 1),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(
						testruntime.MethodEcho,
					),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						// No calls for the first set of backends
						// First backend should get a call.
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
					testruntime.BuildCaller(testruntime.MethodEcho),
					testruntime.NoCallErrors,
					testruntime.AggregateByBackendID(
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
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
					"echo_server",
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
			doAssert: testruntime.MultiAssert(
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-1", 1),
						testruntime.BackendCalledExact("backend-0", 0),
					),
				),
				testruntime.CallOnce(
					"xds:///echo_server",
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
						testruntime.BackendCalledExact("backend-0", 1),
						testruntime.BackendCalledExact("backend-1", 0),
					),
				),
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
