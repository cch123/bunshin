package core

import (
	"context"
	"time"
)

type ClusterPrincipal struct {
	Identity string            `json:"identity,omitempty"`
	Roles    []string          `json:"roles,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type ClusterAuthenticationRequest struct {
	NodeID ClusterNodeID `json:"node_id"`
	Role   ClusterRole   `json:"role"`
}

type ClusterAuthenticator func(context.Context, ClusterAuthenticationRequest) (ClusterPrincipal, error)

type ClusterAuthorizationAction string

const (
	ClusterAuthorizationActionOpenSession    ClusterAuthorizationAction = "open_session"
	ClusterAuthorizationActionIngress        ClusterAuthorizationAction = "ingress"
	ClusterAuthorizationActionScheduleTimer  ClusterAuthorizationAction = "schedule_timer"
	ClusterAuthorizationActionCancelTimer    ClusterAuthorizationAction = "cancel_timer"
	ClusterAuthorizationActionServiceMessage ClusterAuthorizationAction = "service_message"
	ClusterAuthorizationActionSnapshot       ClusterAuthorizationAction = "snapshot"
)

type ClusterAuthorizationRequest struct {
	NodeID        ClusterNodeID              `json:"node_id"`
	Role          ClusterRole                `json:"role"`
	Principal     ClusterPrincipal           `json:"principal,omitempty"`
	Action        ClusterAuthorizationAction `json:"action"`
	SessionID     ClusterSessionID           `json:"session_id,omitempty"`
	CorrelationID ClusterCorrelationID       `json:"correlation_id,omitempty"`
	TimerID       ClusterTimerID             `json:"timer_id,omitempty"`
	Deadline      time.Time                  `json:"deadline,omitempty"`
	SourceService string                     `json:"source_service,omitempty"`
	TargetService string                     `json:"target_service,omitempty"`
	Payload       []byte                     `json:"payload,omitempty"`
}

type ClusterAuthorizer func(context.Context, ClusterAuthorizationRequest) error

type clusterPrincipalContextKey struct{}

func ContextWithClusterPrincipal(ctx context.Context, principal ClusterPrincipal) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, clusterPrincipalContextKey{}, cloneClusterPrincipal(principal))
}

func ClusterPrincipalFromContext(ctx context.Context) (ClusterPrincipal, bool) {
	if ctx == nil {
		return ClusterPrincipal{}, false
	}
	principal, ok := ctx.Value(clusterPrincipalContextKey{}).(ClusterPrincipal)
	if !ok {
		return ClusterPrincipal{}, false
	}
	return cloneClusterPrincipal(principal), true
}

func (n *ClusterNode) authenticateClusterPrincipal(ctx context.Context, request ClusterAuthenticationRequest) (ClusterPrincipal, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	authenticator := n.authenticator
	n.mu.Unlock()
	if authenticator != nil {
		principal, err := authenticator(ctx, request)
		if err != nil {
			return ClusterPrincipal{}, err
		}
		return cloneClusterPrincipal(principal), nil
	}
	return clusterPrincipalFromContext(ctx), nil
}

func (n *ClusterNode) authorizeClusterAction(ctx context.Context, request ClusterAuthorizationRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return ErrClusterClosed
	}
	if request.NodeID == 0 {
		request.NodeID = n.nodeID
	}
	if request.Role == "" {
		request.Role = n.role
	}
	authorizer := n.authorizer
	n.mu.Unlock()
	if authorizer == nil {
		return nil
	}
	request.Principal = clusterPrincipalOrContext(request.Principal, ctx)
	request.Payload = cloneBytes(request.Payload)
	return authorizer(ctx, request)
}

func clusterPrincipalFromContext(ctx context.Context) ClusterPrincipal {
	principal, ok := ClusterPrincipalFromContext(ctx)
	if !ok {
		return ClusterPrincipal{}
	}
	return principal
}

func clusterPrincipalOrContext(principal ClusterPrincipal, ctx context.Context) ClusterPrincipal {
	if principal.Identity != "" || len(principal.Roles) > 0 || len(principal.Metadata) > 0 {
		return cloneClusterPrincipal(principal)
	}
	return clusterPrincipalFromContext(ctx)
}

func cloneClusterPrincipal(principal ClusterPrincipal) ClusterPrincipal {
	principal.Roles = append([]string(nil), principal.Roles...)
	if principal.Metadata != nil {
		metadata := make(map[string]string, len(principal.Metadata))
		for key, value := range principal.Metadata {
			metadata[key] = value
		}
		principal.Metadata = metadata
	}
	return principal
}
