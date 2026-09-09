package callbackstore

// Store keeps the common single-ID case inside the request state.
// Its owner holds cbMu for every operation, but releases it before invoking
// a callback. A registered nil callback is distinct from an absent ID.
type Store[C any] struct {
	firstID string
	first   C
	present bool
	many    map[string]C
}

func (s *Store[C]) Set(id string, callback C) {
	if s.many != nil {
		s.many[id] = callback
		return
	}
	if !s.present || s.firstID == id {
		s.firstID, s.first, s.present = id, callback, true
		return
	}
	s.many = map[string]C{s.firstID: s.first, id: callback}
	var zero C
	s.firstID, s.first, s.present = "", zero, false
}

func (s *Store[C]) Get(id string) (C, bool) {
	if s.many != nil {
		callback, ok := s.many[id]
		return callback, ok
	}
	if s.present && s.firstID == id {
		return s.first, true
	}
	var zero C
	return zero, false
}

func (s *Store[C]) Remove(id string) bool {
	if s.many != nil {
		_, ok := s.many[id]
		delete(s.many, id)
		return ok
	}
	if !s.present || s.firstID != id {
		return false
	}
	var zero C
	s.firstID, s.first, s.present = "", zero, false
	return true
}
