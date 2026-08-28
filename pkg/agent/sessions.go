package agent

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/nats-agent/pkg/wire"
)

// ErrSessionNotFound must be returned (or wrapped) by SessionStore methods
// when the session does not exist for that user; it maps to 404/4041 on the
// wire.
var ErrSessionNotFound = errors.New("session not found")

// SessionStore backs the optional sessions.* endpoints. Implementations own
// persistence entirely (the copay agent uses DynamoDB); the protocol only
// defines the shapes.
type SessionStore interface {
	List(ctx context.Context, userID string) ([]wire.SessionMeta, error)
	Get(ctx context.Context, userID, sessionID string) (*wire.SessionGetResponse, error)
	Delete(ctx context.Context, userID, sessionID string) error
	Rename(ctx context.Context, userID, sessionID, title string) error
	SetFavorite(ctx context.Context, userID, sessionID string, favorite bool) error
}

const sessionOpTimeout = 30 * time.Second

func (a *Agent) sessionRegistrations() []nats_service.EndpointRegistration {
	return []nats_service.EndpointRegistration{
		{
			Path:        "sessionsList",
			Description: "List the user's sessions, favorites first then by recency. Request: {userId}.",
			Response:    &nats_service.ResponseDoc{Description: "SessionsListResponse", ContentType: "application/json"},
			Handler:     a.handleSessionsList,
		},
		{
			Path:        "sessionsGet",
			Description: "Fetch one session's transcript. Request: {userId, sessionId}.",
			Response:    &nats_service.ResponseDoc{Description: "SessionGetResponse", ContentType: "application/json"},
			Handler:     a.handleSessionsGet,
		},
		{
			Path:        "sessionsDelete",
			Description: "Delete a session and its transcript. Request: {userId, sessionId}.",
			Response:    &nats_service.ResponseDoc{Description: "SessionDeleteResponse", ContentType: "application/json"},
			Handler:     a.handleSessionsDelete,
		},
		{
			Path:        "sessionsRename",
			Description: "Rename a session. Request: {userId, sessionId, title}.",
			Response:    &nats_service.ResponseDoc{Description: "SessionRenameResponse", ContentType: "application/json"},
			Handler:     a.handleSessionsRename,
		},
		{
			Path:        "sessionsSetFavorite",
			Description: "Set or clear a session's favorite flag. Request: {userId, sessionId, favorite}.",
			Response:    &nats_service.ResponseDoc{Description: "SessionSetFavoriteResponse", ContentType: "application/json"},
			Handler:     a.handleSessionsSetFavorite,
		},
	}
}

// authorizeSession applies IDT enforcement to a sessions endpoint. sessionID
// may be "" (list). On a verified identity the body's userId is replaced.
func (a *Agent) authorizeSession(msg *nats_service.NatsMessage, userID *string, sessionID string) *nats_service.NatsServiceError {
	id, aerr := a.idt.authorize(msg.Header.Get(wire.HeaderIDT), sessionID)
	if aerr != nil {
		return aerr
	}
	if id.Verified {
		*userID = id.UserID
	}
	return nil
}

func sessionError(err error) *nats_service.NatsServiceError {
	if errors.Is(err, ErrSessionNotFound) {
		return &nats_service.NatsServiceError{
			Status:        404,
			ApiStatusCode: wire.CodeUnknownSession,
			ErrorMessage:  "session not found",
		}
	}
	e := nats_service.NewServerError("session store error", wire.CodeInternal, err)
	return &e
}

func parseSessionReq[T any](msg *nats_service.NatsMessage, validate func(*T) string) (*T, *nats_service.NatsServiceError) {
	var req T
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		e := nats_service.NewValidationError("invalid request body", wire.CodeInvalidBody, err)
		return nil, &e
	}
	if missing := validate(&req); missing != "" {
		e := nats_service.NewValidationError(missing+" is required", wire.CodeMissingField, nil)
		return nil, &e
	}
	return &req, nil
}

func respond(msg *nats_service.NatsMessage, body any) *nats_service.NatsServiceError {
	data, err := json.Marshal(body)
	if err != nil {
		e := nats_service.NewServerError("marshaling response", wire.CodeInternal, err)
		return &e
	}
	msg.ResponseBody = data
	return nil
}

func (a *Agent) handleSessionsList(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	req, verr := parseSessionReq(msg, func(r *wire.SessionsListRequest) string {
		if r.UserID == "" {
			return "userId"
		}
		return ""
	})
	if verr != nil {
		return verr
	}
	if aerr := a.authorizeSession(msg, &req.UserID, ""); aerr != nil {
		return aerr
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	sessions, err := a.sessions.List(ctx, req.UserID)
	if err != nil {
		return sessionError(err)
	}
	if sessions == nil {
		sessions = []wire.SessionMeta{}
	}
	return respond(msg, wire.SessionsListResponse{Sessions: sessions})
}

func (a *Agent) handleSessionsGet(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	req, verr := parseSessionReq(msg, func(r *wire.SessionGetRequest) string {
		if r.UserID == "" {
			return "userId"
		}
		if r.SessionID == "" {
			return "sessionId"
		}
		return ""
	})
	if verr != nil {
		return verr
	}
	if aerr := a.authorizeSession(msg, &req.UserID, req.SessionID); aerr != nil {
		return aerr
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	resp, err := a.sessions.Get(ctx, req.UserID, req.SessionID)
	if err != nil {
		return sessionError(err)
	}
	if resp.Messages == nil {
		resp.Messages = []wire.StoredMessage{}
	}
	return respond(msg, resp)
}

func (a *Agent) handleSessionsDelete(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	req, verr := parseSessionReq(msg, func(r *wire.SessionDeleteRequest) string {
		if r.UserID == "" {
			return "userId"
		}
		if r.SessionID == "" {
			return "sessionId"
		}
		return ""
	})
	if verr != nil {
		return verr
	}
	if aerr := a.authorizeSession(msg, &req.UserID, req.SessionID); aerr != nil {
		return aerr
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	if err := a.sessions.Delete(ctx, req.UserID, req.SessionID); err != nil {
		return sessionError(err)
	}
	return respond(msg, wire.SessionDeleteResponse{SessionID: req.SessionID, Deleted: true})
}

func (a *Agent) handleSessionsRename(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	req, verr := parseSessionReq(msg, func(r *wire.SessionRenameRequest) string {
		if r.UserID == "" {
			return "userId"
		}
		if r.SessionID == "" {
			return "sessionId"
		}
		if r.Title == "" {
			return "title"
		}
		return ""
	})
	if verr != nil {
		return verr
	}
	if aerr := a.authorizeSession(msg, &req.UserID, req.SessionID); aerr != nil {
		return aerr
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	if err := a.sessions.Rename(ctx, req.UserID, req.SessionID, req.Title); err != nil {
		return sessionError(err)
	}
	return respond(msg, wire.SessionRenameResponse{SessionID: req.SessionID, Title: req.Title})
}

func (a *Agent) handleSessionsSetFavorite(msg *nats_service.NatsMessage) *nats_service.NatsServiceError {
	req, verr := parseSessionReq(msg, func(r *wire.SessionSetFavoriteRequest) string {
		if r.UserID == "" {
			return "userId"
		}
		if r.SessionID == "" {
			return "sessionId"
		}
		return ""
	})
	if verr != nil {
		return verr
	}
	if aerr := a.authorizeSession(msg, &req.UserID, req.SessionID); aerr != nil {
		return aerr
	}
	ctx, cancel := context.WithTimeout(context.Background(), sessionOpTimeout)
	defer cancel()
	if err := a.sessions.SetFavorite(ctx, req.UserID, req.SessionID, req.Favorite); err != nil {
		return sessionError(err)
	}
	return respond(msg, wire.SessionSetFavoriteResponse{SessionID: req.SessionID, Favorite: req.Favorite})
}
