package registry

import "log"

// Server 服务端
type Server struct {
	Name string // 服务名

	registry Registry
}

func NewServer(Name string, registry Registry) *Server {
	return &Server{
		Name:     Name,
		registry: registry,
	}
}

func (s *Server) Register() {
	_ = s.registry.Register(s.Name, []byte(s.Name))
	log.Println(s.Name, " online")
}

func (s *Server) Unregister() {
	_ = s.registry.Unregister(s.Name)
	log.Println(s.Name, " offline")
}

func (s *Server) Business(fn func()) {
	fn()
}
