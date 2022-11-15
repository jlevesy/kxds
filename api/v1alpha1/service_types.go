/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"path"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// K8sPort represents a reference to a port. This could be done either by number or by name.
// +kubebuilder:validation:MaxProperties:=1
type K8sPort struct {
	// +optional
	Number int32 `json:"number,omitempty"`
	// +optional
	Name string `json:"name,omitempty"`
}

// K8sService is a reference to a kubernetes service.
type K8sService struct {
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +kubebuilder:validation:Required
	Port K8sPort `json:"port,omitempty"`
}

// Locality is a logical group of endpoints for a given cluster.
// Used for failover mechanisms and weighed locality round robin.
type Locality struct {
	// Weight of the locality, defaults to one.
	// +optional
	// +kubebuilder:default:=1
	Weight uint32 `json:"weight,omitempty"`
	// Priority of the locality, if defined, all entries must unique for a given priority and priority should be defined without any gap.
	// +optional
	Priority uint32 `json:"priority,omitempty"`
	// Services is a reference to a kubernetes service.
	// +optional
	Service *K8sService `json:"service,omitempty"`
}

// Cluster is a group of backend servers serving the same services.
type Cluster struct {
	// Name is the name of the Cluster
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	// MaxRequests qualifies the maximum number of parallel requests allowd to the upstream cluster.
	MaxRequests *uint32 `json:"maxRequests,omitempty"`
	// +kubebuilder:validation:MinItems:=1
	Localities []Locality `json:"localities,omitempty"`
}

// ClusterRef is a reference to a cluter defined in the same manifest.
type ClusterRef struct {
	// Name is the name of the Cluster
	// +kubebuilder:validation:Required
	Name string `json:"name,omitempty"`
	// Weight is the weight of this cluster.
	// +optional
	// +kubebuilder:default:=1
	Weight uint32 `json:"weight,omitempty"`
}

type RegexMatcher struct {
	// Regexp to evaluate the path against.
	Regex string `json:"regex,omitempty"`
	// The regexp engine to use.
	// +kubebuilder:validation:Enum:=re2
	// +kubebuilder:default:=re2
	Engine string `json:"engine,omitempty"`
}

type RangeMatcher struct {
	// Start of the range (inclusive)
	Start int64 `json:"start,omitempty"`
	// End of the range (exclusive)
	End int64 `json:"end,omitempty"`
}

// PathMatcher indicates a match based on the path of a gRPC call.
type PathMatcher struct {
	// Path Must match the prefix of the request.
	// +optional
	// +kubebuilder:default:=/
	Prefix string `json:"prefix,omitempty"`
	// Path Must match exactly.
	// +optional
	Path string `json:"path,omitempty"`
	// Path Must Match a Regex.
	// +optional
	Regex *RegexMatcher `json:"regex,omitempty"`
}

// HeaderMatcher indicates a match based on an http header.
type HeaderMatcher struct {
	// Name of the header to match.
	Name string `json:"name,omitempty"`
	// Match the exact value of a header.
	Exact *string `json:"exact,omitempty"`
	// Match a regex. Must match the whole value.
	Regex *RegexMatcher `json:"regex,omitempty"`
	// Header Value must match a range.
	Range *RangeMatcher `json:"range,omitempty"`
	// Header must be present.
	Present *bool `json:"present,omitempty"`
	// Header value must have a prefix.
	Prefix *string `json:"prefix,omitempty"`
	// Header value must have a suffix.
	Suffix *string `json:"suffix,omitempty"`
	// Invert that header match.
	Invert bool `json:"invert,omitempty"`
}

type Fraction struct {
	// Numerator of the fraction
	Numerator uint32 `json:"numerator,omitempty"`
	// Denominator of the fration.
	// +kubebuilder:validation:Enum:=hundred;ten_thousand;million
	// +kubebuilder:default:=hundred
	Denominator string `json:"denominator,omitempty"`
}

type HeaderFault struct{}

type FaultDelay struct {
	// FixedDelay adds a fixed delay before a call.
	Fixed *metav1.Duration `json:"fixed,omitempty"`
	// Header adds a delay controlled by an HTTP header.
	Header *HeaderFault `json:"header,omitempty"`
	// Percentage controls how much this fault delay will be injected.
	Percentage *Fraction `json:"percentage,omitempty"`
}

type FaultAbort struct {
	// Returns the HTTP status code.
	HTTPStatus *uint32 `json:"http,omitempty"`
	// Returns the gRPC status code.
	GRPCStatus *uint32 `json:"grpc,omitempty"`
	// Header adds a fault controlled by an HTTP header.
	Header *HeaderFault `json:"header,omitempty"`
	// Percentage controls how much this fault delay will be injected.
	Percentage *Fraction `json:"percentage,omitempty"`
}

type FaultFilter struct {
	// Inject a delay.
	Delay *FaultDelay `json:"delay,omitempty"`
	// Abort the call.
	Abort *FaultAbort `json:"abort,omitempty"`
	// The maximum number of faults that can be active at a single time.
	MaxActiveFaults *uint32 `json:"maxActiveFaults,omitempty"`
	// Specifies a set of headers that the filter should match on.
	Headers []HeaderMatcher `json:"headers,omitempty"`
}

type FilterConfig struct {
	// Fault Filter configuration.
	// +optional
	Fault *FaultFilter `json:"fault,omitempty"`
}

type Filter struct {
	// Name of the filter.
	Name string `json:"name,omitempty"`

	FilterConfig `json:",inline"`
}

// Route allows to match an outoing request to a specific cluster, it allows to do HTTP level manipulation on the outgoing requests as well as matching.
type Route struct {
	// Path allows to specfies path matcher for a specific route.
	Path PathMatcher `json:"path,omitempty"`
	// Headers allows to match on a specific set of headers.
	Headers []HeaderMatcher `json:"headers,omitempty"`
	// Indicates if the matching should be case sensitive.
	// +kubebuilder:default:=true
	CaseSensitive bool `json:"caseSensitive,omitempty"`
	// Only handle a fraction of matching requests.
	RuntimeFraction *Fraction `json:"fraction,omitempty"`
	// Specifies the maximum duration allowed for streams on the route.
	MaxStreamDuration *metav1.Duration `json:"maxStreamDuration,omitempty"`
	// Specifies the maximum duration allowed for streams on the route.
	// If present, and the request contains a `grpc-timeout header
	// <https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md>`_, use that value as the
	// *max_stream_duration*, but limit the applied timeout to the maximum value specified here.
	// If set to 0, the `grpc-timeout` header is used without modification.
	GrpcTimeoutHeaderMax *metav1.Duration `json:"grpcTimeoutHeaderMax,omitempty"`
	// Cluster carries the reference to a cluster name.
	Clusters []ClusterRef `json:"clusters,omitempty"`
}

// XDSServiceSpec defines the desired state of Service
type XDSServiceSpec struct {
	// MaxStreamDuration is the total duration to keep alive an HTTP request/response stream.
	// If the time limit is reached the stream will be reset independent of any other timeouts.
	// If not specified, this value is not set.
	MaxStreamDuration *metav1.Duration `json:"maxStreamDuration,omitempty"`
	// Filters represent the list of filters applied in that service.
	// +optional
	Filters []Filter `json:"filters,omitempty"`
	// Routes lists all the routes defined for an XDSService.
	// +kubebuilder:validation:MinItems:=1
	Routes []Route `json:"routes,omitempty"`
	// Routes lists all the  clusters defined for an XDSService.
	// +kubebuilder:validation:MinItems:=1
	Clusters []Cluster `json:"clusters,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// XDSService is the Schema for the services API
type XDSService struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec XDSServiceSpec `json:"spec,omitempty"`
}

func (s *XDSService) ResourcePrefix() string {
	return "kxds" + "." + s.Name + "." + s.Namespace
}

func (s *XDSService) ListenerName() string {
	return path.Join(s.ObjectMeta.Namespace, s.ObjectMeta.Name)
}

func (s *XDSService) RouteConfigName() string {
	return s.ResourcePrefix() + ".routeconfig"
}

//+kubebuilder:object:root=true

// ServiceList contains a list of Service
type XDSServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []XDSService `json:"items"`
}

func init() {
	SchemeBuilder.Register(&XDSService{}, &XDSServiceList{})
}
