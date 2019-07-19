package prometheusdb

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
)


type HTTPServer struct {
	addr string
	s    *Server
}

func (s *HTTPServer) Start() error {
	mux := http.NewServeMux()
	mux.Handle("/write", http.HandlerFunc(s.Write))
	mux.Handle("/query", http.HandlerFunc(s.Query))
	return http.ListenAndServe(s.addr, mux)
}



func (s *HTTPServer) Write(resp http.ResponseWriter, req *http.Request) {
	reader := bufio.NewReader(req.Body)
	defer req.Body.Close()
	var reqs []WriteReq
	for {
		j, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		if len(j) == 0 {
			continue
		}
		writeReq := WriteReq{}
		err = json.Unmarshal(j, writeReq)
		if err != nil {
			return
		}
		reqs = append(reqs, writeReq)
	}
	_ = s.s.Write(reqs...)
}

func (s *HTTPServer) Query(resp http.ResponseWriter, req *http.Request) {

}