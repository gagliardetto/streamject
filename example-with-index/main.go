package main

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	rtd "runtime/debug"

	"github.com/gagliardetto/streamject"
	jsoniter "github.com/json-iterator/go"
)

func init() {
	jsoniter.RegisterFieldDecoderFunc(reflect.TypeOf(CircleCIBuildPartialInfo{}).String(), "BuildNum", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		i := iter.ReadInt()
		*((*int)(ptr)) = i
	})
}

type CircleCIBuildPartialInfo struct {
	//AuthorDate      time.Time     `json:"author_date"`
	//AuthorEmail     string        `json:"author_email"`
	//AuthorName      string        `json:"author_name"`
	//Body            string        `json:"body"`
	//Branch          string        `json:"branch"`
	BuildNum int         `json:"build_num" yaml:"id" msgpack:"id"` // VERY IMPORTANT!
	StopTime *CustomTime `json:"stop_time" yaml:"fi,omitempty" msgpack:"-"`
	//BuildTimeMillis int           `json:"build_time_millis"`
	//BuildURL        string        `json:"build_url"`
	//CommitterDate   time.Time     `json:"committer_date"`
	//CommitterEmail  string        `json:"committer_email"`
	//CommitterName   string        `json:"committer_name"`
	//DontBuild       interface{}   `json:"dont_build"`
	//Fleet           string        `json:"fleet"`
	//Lifecycle string `json:"lifecycle"`
	//Outcome   string `json:"outcome"`
	//Parallel        int           `json:"parallel"`
	//Platform        string        `json:"platform"`
	//PullRequests    []interface{} `json:"pull_requests"`
	//QueuedAt string `json:"queued_at"`
	//Reponame        string        `json:"reponame"`
	//StartTime       time.Time     `json:"start_time"`
	//Status   string    `json:"status"`
	//Subject         string        `json:"subject"`
	//UsageQueuedAt   string        `json:"usage_queued_at"`
	//User            struct {
	//	AvatarURL string `json:"avatar_url"`
	//	ID        int    `json:"id"`
	//	IsUser    bool   `json:"is_user"`
	//	Login     string `json:"login"`
	//	Name      string `json:"name"`
	//	VcsType   string `json:"vcs_type"`
	//} `json:"user"`
	//Username    string      `json:"username"`
	//VcsRevision string      `json:"vcs_revision"`
	//VcsTag      interface{} `json:"vcs_tag"`
	//VcsURL      string      `json:"vcs_url"`
	//Why         string      `json:"why"`
	//Workflows   struct {
	//	JobID                  string        `json:"job_id"`
	//	JobName                string        `json:"job_name"`
	//	UpstreamConcurrencyMap struct{}      `json:"upstream_concurrency_map"`
	//	UpstreamJobIds         []interface{} `json:"upstream_job_ids"`
	//	WorkflowID             string        `json:"workflow_id"`
	//	WorkflowName           string        `json:"workflow_name"`
	//	WorkspaceID            string        `json:"workspace_id"`
	//} `json:"workflows"`
}

type CustomTime struct {
	time.Time
}

func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ct.Time = time.Time{}
		return nil
	}
	var err error
	// try parsing as RFC3339:
	ct.Time, err = time.Parse(time.RFC3339, s)
	if err != nil {
		// if cannot parse as RFC3339, try parsing as unix timestamp:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		ct.Time = time.Unix(i, 0)
	}
	return nil
}

func (ct *CustomTime) MarshalJSON() ([]byte, error) {
	if ct.Time.UnixNano() == nilTime {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.Time.Format(time.RFC3339))), nil
}

var nilTime = (time.Time{}).UnixNano()

func (ct *CustomTime) IsSet() bool {
	return ct.UnixNano() != nilTime
}
func formatNilCustomTime(t *CustomTime) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.Time
}

func main() {

	fn := "./owner.buildlist.json"
	//defer os.Remove(fn)
	stm, err := streamject.NewJSON(fn)
	if err != nil {
		panic(err)
	}
	defer stm.Close()

	start := time.Now()

	if false {
		for i := 0; i < 1000000; i++ {
			newItem := &CircleCIBuildPartialInfo{
				BuildNum: i,
			}
			//HasCircleCIBuildPartialInfo(stm, 2)
			err = stm.Append(newItem)
			if err != nil {
				panic(err)
			}
		}
	}
	fmt.Println("added all in:", time.Now().Sub(start))

	start = time.Now()
	err = stm.Iterate(func(line streamject.Line) bool {
		var msg CircleCIBuildPartialInfo
		err := line.Decode(&msg)
		if err != nil {
			panic(err)
		}
		// WARNING: FreeOSMemory() takes a lot of time.
		//rtd.FreeOSMemory()
		//spew.Dump(msg)
		return true
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("iterated all in:", time.Now().Sub(start))

	has := HasCircleCIBuildPartialInfo(stm, 2)
	fmt.Println("has:", has)
	has = HasCircleCIBuildPartialInfo(stm, 1)
	fmt.Println("has:", has)
	has = HasCircleCIBuildPartialInfo(stm, 3)
	fmt.Println("has:", has)
	has = HasCircleCIBuildPartialInfo(stm, 4)
	fmt.Println("has:", has)

	has = HasCircleCIBuildPartialInfo(stm, 99000000)
	fmt.Println("has:", has)
	// TODO:
	// - parse line json.
	// - obj is kept in memory until this function is not exited.
	// - when this function exits, the object is destroyed, and GC is performed.
	// - this means that any use of the object must be done inside this funtion.
	// -
	// -
	// -

	// NOTES:
	// - the object will be kept in memory as long as this function is not closed.
	// - it's up to you to load as many lines as you want or can, and close them.
	rtd.FreeOSMemory()

	fmt.Println("all done in", time.Now().Sub(start))
	time.Sleep(time.Minute)
}

func HasCircleCIBuildPartialInfo(stm *streamject.Stream, buildID int) bool {

	indexName := "circleci.BuildNum"

	stm.CreateIndexOnInt(indexName, func(line streamject.Line) int {
		var build CircleCIBuildPartialInfo
		err := line.Decode(&build)
		if err != nil {
			panic(err)
		}

		return build.BuildNum
	})

	return stm.HasIntByIndex(indexName, buildID)
}
