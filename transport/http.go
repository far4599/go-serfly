// Implementation of http raft transport.

package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/far4599/go-serfly/types"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/serf"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

const (
	requestVotePath     = "/request_vote"
	leaderHeartbeatPath = "/leader_heartbeat"

	addrTagName = "http_transport_addr"
)

func NewHttpTransport(addr string, port int, client *http.Client, logger *zap.Logger) *HttpTransport {
	if client == nil {
		client = http.DefaultClient
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	t := &HttpTransport{
		addr: addr,
		port: port,

		client: client,

		logger: logger,
	}

	r := mux.NewRouter()
	r.Methods("POST").Path(requestVotePath).HandlerFunc(t.handleRequest(requestVotePath))
	r.Methods("POST").Path(leaderHeartbeatPath).HandlerFunc(t.handleRequest(leaderHeartbeatPath))

	t.router = r

	return t
}

type HttpTransport struct {
	addr    string
	port    int
	closeCh chan struct{}

	router *mux.Router
	server *http.Server

	client *http.Client

	requestVoteHandlerFn     func(ctx context.Context, req types.RequestVoteReq) (*types.RequestVoteResp, error)
	leaderHeartbeatHandlerFn func(ctx context.Context, req types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error)

	logger *zap.Logger
}

func (h *HttpTransport) SendVoteRequest(ctx context.Context, target serf.Member, msg types.RequestVoteReq) (*types.RequestVoteResp, error) {
	respBody, err := h.makeRequest(ctx, target, requestVotePath, msg)
	if err != nil {
		return nil, err
	}

	var resp types.RequestVoteResp
	if err = jsoniter.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (h *HttpTransport) SendLeaderHeartbeat(ctx context.Context, target serf.Member, msg types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error) {
	respBody, err := h.makeRequest(ctx, target, leaderHeartbeatPath, msg)
	if err != nil {
		return nil, err
	}

	var resp types.LeaderHeartbeatResp
	if err = jsoniter.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (h *HttpTransport) SetRequestVoteHandler(f func(ctx context.Context, req types.RequestVoteReq) (*types.RequestVoteResp, error)) error {
	h.requestVoteHandlerFn = f
	return nil
}

func (h *HttpTransport) SetLeaderHeartbeatHandler(f func(ctx context.Context, req types.LeaderHeartbeatReq) (*types.LeaderHeartbeatResp, error)) error {
	h.leaderHeartbeatHandlerFn = f
	return nil
}

// AddTransportTags adds tag with peer address
func (h *HttpTransport) AddTransportTags(tags map[string]string) map[string]string {
	if tags == nil {
		tags = make(map[string]string)
	}
	tags[addrTagName] = fmt.Sprintf("%s:%d", h.addr, h.port)
	return tags
}

// Start starts the http server
func (h *HttpTransport) Start() error {
	h.server = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", h.addr, h.port),
		Handler: handlers.RecoveryHandler()(h.router),
	}

	if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	return nil
}

// Stop gracefully shuts down server
func (h *HttpTransport) Stop() error {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()

	err := h.server.Shutdown(ctx)
	if err != nil {
		return err
	}

	return nil
}

// GetRouter is a getter of mux to modify it if needed
func (h *HttpTransport) GetRouter() *mux.Router {
	return h.router
}

func (h *HttpTransport) handleRequest(path string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			err               error
			reqBody, respBody []byte

			l = h.logger.With(zap.String("path", path))
		)

		defer func() {
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, err = w.Write([]byte(err.Error()))
				if err != nil {
					l.Error("failed to write response body", zap.Error(err))
				}
			}
		}()

		reqBody, err = io.ReadAll(r.Body)
		if err != nil {
			l.Error("failed to read request body", zap.Error(err))
			return
		}
		defer r.Body.Close()

		var resp interface{}
		if path == leaderHeartbeatPath {
			var req types.LeaderHeartbeatReq
			if err = jsoniter.Unmarshal(reqBody, &req); err != nil {
				l.Error("failed to unmarshal request", zap.Error(err))
				return
			}

			resp, err = h.leaderHeartbeatHandlerFn(context.Background(), req)
			if err != nil {
				l.Error("failed to handle request", zap.Error(err))
				return
			}
		} else if path == requestVotePath {
			var req types.RequestVoteReq
			if err = jsoniter.Unmarshal(reqBody, &req); err != nil {
				l.Error("failed to unmarshal request", zap.Error(err))
				return
			}

			resp, err = h.requestVoteHandlerFn(context.Background(), req)
			if err != nil {
				l.Error("failed to handle request", zap.Error(err))
				return
			}
		}

		if resp == nil {
			l.Error("response is nil", zap.Error(err))
			return
		}

		respBody, err = jsoniter.Marshal(resp)
		if err != nil {
			l.Error("failed to marshal response", zap.Error(err))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, err = w.Write(respBody)
	}
}

func (h *HttpTransport) makeRequest(ctx context.Context, target serf.Member, path string, msg interface{}) ([]byte, error) {
	reqBody, err := jsoniter.Marshal(msg)
	if err != nil {
		return nil, err
	}

	targetAddr, ok := target.Tags[addrTagName]
	if !ok {
		return nil, fmt.Errorf("target addres is not specified")
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", targetAddr, path), bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}

	res, err := h.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	return respBody, nil
}
