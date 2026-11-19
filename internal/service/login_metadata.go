package service

// removeSessionLocked keeps bounded insertion order consistent with CacheTable.
func (s *Service) removeSessionLocked(key int64) {
	delete(s.sessions, key)
	for i, v := range s.sessionOrder {
		if v == key {
			s.sessionOrder = append(s.sessionOrder[:i], s.sessionOrder[i+1:]...)
			break
		}
	}
}

// metadataLoginUsers is getLoginUserList: the sessions still valid under the
// liveness rule (sessions.go), in login order. Expired ones are removed first.
func (s *Service) metadataLoginUsers() []MetadataLoginUser {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.purgeExpiredLocked(s.config.Clock())
	out := make([]MetadataLoginUser, 0, len(s.sessionOrder))
	for _, key := range s.sessionOrder {
		out = append(out, s.sessions[key].user)
	}
	return out
}

// LoginUser is LoginManager.getUser(session) for RequestLogger: the login id
// and the LOGIN "ip" of a session still in the table. It does not extend or
// expire the session.
func (s *Service) LoginUser(session int64) (MetadataLoginUser, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.sessions[session]
	if !ok {
		return MetadataLoginUser{}, false
	}
	return e.user, true
}
