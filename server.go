package prometheusdb

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
	//tsdbLabels "github.com/prometheus/tsdb/labels"
	"github.com/prometheus/prometheus/pkg/labels"

	tsdbStorage "github.com/prometheus/prometheus/storage/tsdb"
)

type Server struct {
	db     storage.Storage
	engine *promql.Engine
}

type Label = labels.Label
type Labels = labels.Labels
type Value = promql.Value
type Result = promql.Result

func NewServer(conf Config) (*Server, error) {
	db, err := tsdb.Open(conf.Dir, nil, nil, tsdb.DefaultOptions) //prometheus.DefaultRegisterer
	if err != nil {
		return nil, err
	}
	engine := promql.NewEngine(promql.EngineOpts{
		Logger:        nil, // log.With(logger, "component", "query engine"),
		Reg:           nil, //prometheus.DefaultRegisterer,
		MaxConcurrent: 10,
		MaxSamples:    10000000,
		Timeout:       time.Duration(50 * time.Second),
	})
	st := &tsdbStorage.ReadyStorage{}
	st.Set(db, int64(time.Duration(2 * time.Hour) / time.Millisecond))
	return &Server{ db: st, engine: engine }, nil
}

type WriteResult struct {
	Ref uint64
}

type WriteReq struct {
	Labels  Labels    `json:"labels"`
	Time    int64     `json:"time"`
	Value   float64   `json:"value"`
}

func (s *Server) Write(reqs ...WriteReq) error {
	var err error
	var appender storage.Appender
	appender, err = s.db.Appender()
	if err != nil {
		return err
	}
	//fmt.Println(reqs)
	for _, req := range reqs {
		_, err = appender.Add(req.Labels, req.Time, req.Value)
		if err != nil {
			break
			//return WriteResult{}, err
		}
	}
	if err != nil {
		_ = appender.Rollback()
		return err
	} else {
		return appender.Commit()
	}
	//
	//err = appender.Commit()
	//if err != nil {
	//	return WriteResult{}, err
	//}
	//return WriteResult{ Ref: 0 }, nil
}

func (s *Server) RangeQuery(query string, start, end time.Time, interval time.Duration) (*Result, error) {
	q, err := s.engine.NewRangeQuery(s.db, query, start, end, interval)
	if err != nil {
		return nil, err
	}
	result := q.Exec(context.Background())
	q.Close()
	return result, nil
	//defer q.Close()
	//if result.Err != nil {
	//	return nil, result.Err
	//}
	//return result.Value, nil
	//return json.Marshal(result.Value)
}

func (s *Server) InstantQuery(query string, ts time.Time) (*Result, error) {
	q, err := s.engine.NewInstantQuery(s.db, query, ts)
	if err != nil {
		return nil, err
	}
	result := q.Exec(context.Background())
	q.Close()
	return result, nil
	//
	//if result.Err != nil {
	//	return nil, result.Err
	//}
	//return json.Marshal(result.Value)
}

func (s *Server) Close() {
	_ = s.db.Close()
}
