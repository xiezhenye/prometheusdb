package prometheusdb

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestReadWrite(t *testing.T) {
	server, err := NewServer(Config {
		Dir: "/tmp/test_ptsdb",
	})
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer server.Close()

	now := time.Now()
	nanoTs := now.UnixNano()
	rand.Seed(nanoTs)
	ts := nanoTs / int64(time.Millisecond)
	v := float64(rand.Intn(1000))
	err = server.Write(WriteReq{
		Labels{
			Label{Name: "__name__", Value: "test"},
			Label{Name: "app", Value: "ic"},
			Label{Name: "idc", Value:"bd"},
		},
		ts,
		v,
	})
	if err != nil {
		t.Error(err.Error())
		return
	}
	err = server.Write(WriteReq{
		Labels{Label{Name: "__name__", Value: "test"}, Label{Name: "app", Value: "ic"}, Label{Name: "idc", Value:"bd"}},
		ts - 2000,
		v,
	})
	//if err != nil {
	//	t.Error(err.Error())
	//	return
	//}

	result, err := server.RangeQuery("test{}", now.Add(-time.Second * 2), now, time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if result.Err != nil {
		t.Error(result.Err.Error())
	}
	//fmt.Println(result.Warnings)

	j, _ := json.Marshal(result.Value)
	fmt.Println(string(j))

	result, err = server.InstantQuery("test{}", now)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if result.Err != nil {
		t.Error(result.Err.Error())
	}
	//fmt.Println(result.Warnings)
	j, _ = json.Marshal(result.Value)
	fmt.Println(string(j))
}

func TestAggr(t *testing.T) {
	server, err := NewServer(Config {
		Dir: "/tmp/test_ptsdb",
	})
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer server.Close()

	now := time.Now()
	nanoTs := now.UnixNano()
	rand.Seed(nanoTs)
	millTs := nanoTs / int64(time.Millisecond)
	v := float64(rand.Intn(1000))
	apps := []string{"ic","oc","ump","sc","pay","rds","nsq","kvds","dts"}
	idcs := []string{"bd","bj2"}
	d := int64(60)
	for ts := millTs - d * 1000; ts <= millTs; ts+= 1000 {
		reqs := make([]WriteReq, 0, 10000)
		for i := 0; i < 10000; i++ {
			n := int(rand.NormFloat64() * 1000 + 10000)
			reqs = append(reqs, WriteReq{
				Labels{
					Label{Name: "__name__", Value: "test2"},
					Label{Name: "app", Value: apps[rand.Intn(len(apps))]},
					Label{Name: "idc", Value: idcs[rand.Intn(len(idcs))]},
					Label{Name: "t", Value: strconv.Itoa(n)},
				},
				ts,
				v,
			})
		}
		err = server.Write(reqs...)
		if err != nil {
			t.Error(err.Error())
			return
		}
		fmt.Println(ts)
	}
	fmt.Println()
	query(t, server, `count(test2{t="10000"})`, now)
	query(t, server, `count(test2) by (app)`, now)
	query(t, server, `topk(5, count(test2) by (app))`, now)
	query(t, server, `topk(5, count(test2) by (t))`, now)
	query(t, server, `count(test2{app="ic"})`, now)
	queryR(t, server, `count(test2{app="ic"})`, now.Add(-time.Duration(d) * time.Second), now, time.Second)

}

func query(t *testing.T, server *Server, query string, now time.Time){
	fmt.Println(query, now)
	t0 := time.Now()
	result, err := server.InstantQuery(query, now)
	if err != nil {
		t.Error(err.Error())
		return
	}
	fmt.Println(time.Now().Sub(t0))
	if result.Err != nil {
		t.Error(result.Err.Error())
	}
	//fmt.Println(result.Warnings)
	j, _ := json.Marshal(result.Value)
	fmt.Println(string(j))
	fmt.Println()
}

func queryR(t *testing.T, server *Server, query string, from time.Time, to time.Time, interval time.Duration){
	fmt.Println(query, from, to, interval)
	t0 := time.Now()
	result, err := server.RangeQuery(query, from, to, interval)
	if err != nil {
		t.Error(err.Error())
		return
	}
	fmt.Println(time.Now().Sub(t0))
	if result.Err != nil {
		t.Error(result.Err.Error())
	}
	//fmt.Println(result.Warnings)
	j, _ := json.Marshal(result.Value)
	fmt.Println(string(j))
	fmt.Println()
}