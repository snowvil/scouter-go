package webapp

import (
	"context"
	"net/http"

	"github.com/snowvil/scouter-go/internal/protocol"
)

// UserController (layer/controller/UserController.java:48-104).

type loginRequest struct {
	ServerID flexInt `json:"serverId"`
	User     *struct {
		ID       *string `json:"id"`
		Password *string `json:"password"`
	} `json:"user"`
}

// readLogin applies LoginRequest/SUser bean validation (@NotNull user, user.id).
func (s *Server) readLogin(w http.ResponseWriter, r *http.Request, method string) (id, password string, err error) {
	var req loginRequest
	if err := s.readBody(w, r, &req); err != nil {
		return "", "", err
	}
	if req.User == nil {
		return "", "", newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg0.user may not be null")
	}
	if req.User.ID == nil {
		return "", "", newAPIError(http.StatusBadRequest, http.StatusBadRequest, "[ValidationException] "+method+".arg0.user.id may not be null")
	}
	if req.User.Password != nil {
		password = *req.User.Password
	}
	return *req.User.ID, password, nil
}

// checkLogin is AccountConsumer.login: CHECK_LOGIN {id, pass=sha256} -> BooleanValue.
func (s *Server) checkLogin(ctx context.Context, id, password string) error {
	valid, got := false, false
	err := s.call(ctx, "CHECK_LOGIN", newMap(entry("id", textValue(id)), entry("pass", textValue(scouterPassword(password)))), func(it Item) error {
		if !got && it.Value != nil {
			got = true
			valid = it.Value.Type == protocol.ValueBoolean && it.Value.Bool
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !valid {
		return errLoginFail
	}
	return nil
}

// POST /v1/user/login (@NoAuth): CHECK_LOGIN, then the servlet session holds
// userId (cookie JSESSIONID).
func (s *Server) userLogin(w http.ResponseWriter, r *http.Request) {
	id, password, err := s.readLogin(w, r, "login")
	if err == nil {
		err = s.checkLogin(r.Context(), id, password)
	}
	if err == nil {
		err = s.sessions.login(w, r, id, s.now())
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, true)
}

type bearerTokenView struct {
	Success     bool   `json:"success"`
	BearerToken string `json:"bearerToken"`
}

// POST /v1/user/loginGetToken (@NoAuth): CHECK_LOGIN + publishToken.
func (s *Server) userLoginGetToken(w http.ResponseWriter, r *http.Request) {
	id, password, err := s.readLogin(w, r, "login3rdParty")
	if err == nil {
		err = s.checkLogin(r.Context(), id, password)
	}
	var token string
	if err == nil {
		token, err = s.publishToken(r.Context(), id)
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.writeJSON(w, r, bearerTokenView{Success: true, BearerToken: token})
}
