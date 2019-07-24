package main

import (
	"encoding/json"
	"fmt"
	"time"

	rtd "runtime/debug"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/streamject"
	. "github.com/gagliardetto/utils"
)

var rawFaulty = ``

func main() {

	fn := "./owner.buildlist.json"
	//defer os.Remove(fn)
	stm, err := streamject.NewJSON(fn)
	if err != nil {
		panic(err)
	}
	defer stm.Close()

	start := time.Now()

	var build TravisCIBuild
	err = json.Unmarshal([]byte(rawFaulty), &build)
	if err != nil {
		panic(err)
	}

	if false {
		for i := 0; i < 600000; i++ {
			newItem := &Message{
				Name: RandomString(256),
				Text: RandomString(256),
				Sub: &SubMessage{
					Name: RandomString(256),
					Text: RandomString(256),
				},
			}
			err = stm.Append(newItem)
			if err != nil {
				panic(err)
			}
		}
	}

	err = stm.Append(&build)
	if err != nil {
		panic(err)
	}

	err = stm.Iterate(func(line *streamject.Line) bool {
		var msg TravisCIBuild
		err := line.Decode(&msg)
		if err != nil {
			panic(err)
		}

		spew.Dump(msg)
		return true
	})
	if err != nil {
		panic(err)
	}

	has := hasByID(stm, 2)
	fmt.Println("has:", has)
	has = hasByID(stm, 1)
	fmt.Println("has:", has)
	has = hasByID(stm, 3)
	fmt.Println("has:", has)
	has = hasByID(stm, 4)
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

func hasByID(stm *streamject.Stream, id int) bool {

	var has bool
	err := stm.Iterate(func(line *streamject.Line) bool {
		var msg MessageIDOnly
		err := line.Decode(&msg)
		if err != nil {
			panic(err)
		}

		//spew.Dump(msg)
		if msg.ID == id {
			has = true
			return false
		}

		return true
	})
	if err != nil {
		panic(err)
	}
	return has
}

type Message struct {
	Text string `msgpack:"text"`
	Name string `msgpack:"name"`
	ID   int    `msgpack:"id"`
	Sub  *SubMessage
}
type SubMessage struct {
	Text string `msgpack:"text"`
	Name string `msgpack:"name"`
	ID   int    `msgpack:"id"`
}
type MessageIDOnly struct {
	ID int
}

type CiccioMeta struct {
	AddedAt time.Time `yaml:"aa,omitempty" msgpack:"aa,omitempty"`
}
type TravisCIJob struct {
	//Href           string `json:"@href" yaml:"-"`
	//Representation string `json:"@representation" yaml:"-"`
	//Type           string `json:"@type" yaml:"-"`
	ID int `json:"id" msgpack:"id"`
	I  int `json:"-" yaml:"-" msgpack:"-"`
}
type TravisCIBuild struct {
	CiccioMeta *CiccioMeta `yaml:"cm,omitempty" msgpack:"cm,omitempty"`
	//Href        string `json:"@href" yaml:"-"`
	//Permissions struct {
	//Cancel  bool `json:"cancel"`
	//Read    bool `json:"read"`
	//Restart bool `json:"restart"`
	//} `json:"@permissions" yaml:"-"`
	//Representation string `json:"@representation" yaml:"-"`
	//Type           string `json:"@type" yaml:"-"`
	Branch struct {
		//Href           string `json:"@href" yaml:"-"`
		//Representation string `json:"@representation" yaml:"-"`
		//Type           string `json:"@type" yaml:"-"`
		DefaultBranch  bool `json:"default_branch" yaml:"db,omitempty" msgpack:"db,omitempty"`
		ExistsOnGithub bool `json:"exists_on_github" yaml:"eog,omitempty" msgpack:"eog,omitempty"`
		//LastBuild      struct {
		//	Href string `json:"@href"`
		//} `json:"last_build"`
		Name string `json:"name" yaml:"nm,omitempty" msgpack:"nm,omitempty"`
		//Repository struct {
		//Href           string `json:"@href" yaml:"-"`
		//Representation string `json:"@representation" yaml:"-"`
		//Type           string `json:"@type" yaml:"-"`
		//	ID             int    `json:"id"`
		//Name           string `json:"name"`
		//Slug           string `json:"slug" yaml:"-"`
		//} `json:"repository" yaml:"-"`
	} `json:"branch" yaml:"br,omitempty" msgpack:"br,omitempty"`
	Commit struct {
		//Representation string `json:"@representation" yaml:"-"`
		//Type           string `json:"@type" yaml:"-"`
		//Author         struct {
		//AvatarURL string `json:"avatar_url" yaml:"-"`
		//Name      string `json:"name"`
		//} `json:"author" yaml:"-"`
		CommittedAt string `json:"committed_at" yaml:"ca,omitempty" msgpack:"ca,omitempty"`
		//Committer   struct {
		//	AvatarURL string `json:"avatar_url"`
		//	Name      string `json:"name"`
		//} `json:"committer" yaml:"-"`
		CompareURL string `json:"compare_url" yaml:"cu,omitempty" msgpack:"cu,omitempty"`
		ID         int64  `json:"id" yaml:"-" msgpack:"-"`
		Message    string `json:"message" yaml:"m,omitempty" msgpack:"m,omitempty"`
		//Ref        string `json:"ref" yaml:"-"`
		Sha string `json:"sha" yaml:"s,omitempty" msgpack:"s,omitempty"`
	} `json:"commit" yaml:"c,omitempty" msgpack:"c,omitempty"`
	CreatedBy struct {
		//Href        string `json:"@href" yaml:"-"`
		//Permissions struct {
		//	Read bool `json:"read"`
		//	Sync bool `json:"sync"`
		//} `json:"@permissions" yaml:"-"` //TODO: are there other permissions?
		//Representation string `json:"@representation" yaml:"-"`
		//Type           string `json:"@type" yaml:"-"`
		//AllowMigration bool   `json:"allow_migration" yaml:"-"`
		//AvatarURL      string `json:"avatar_url" yaml:"-"`
		//Education      bool   `json:"education" yaml:"-"`
		GithubID int64 `json:"github_id" yaml:"gi,omitempty" msgpack:"gi,omitempty"`
		//ID             int64    `json:"id" yaml:"-"`
		//IsSyncing      bool   `json:"is_syncing" yaml:"-"`
		Login string `json:"login" yaml:"lg,omitempty" msgpack:"lg,omitempty"`
		//Name           string `json:"name" yaml:"-"`
		//SyncedAt       string `json:"synced_at" yaml:"-" yaml:"-"`
	} `json:"created_by" yaml:"cb,omitempty" msgpack:"cb,omitempty"`
	Duration      int64          `json:"duration" yaml:"d,omitempty" msgpack:"d,omitempty"`
	EventType     string         `json:"event_type" yaml:"et,omitempty" msgpack:"et,omitempty"`
	FinishedAt    time.Time      `json:"finished_at" yaml:"fa,omitempty" msgpack:"fa,omitempty"`
	ID            int64          `json:"id" msgpack:"id"`
	Jobs          []*TravisCIJob `json:"jobs" yaml:"j,omitempty" msgpack:"j,omitempty"`
	Number        string         `json:"number" yaml:"n,omitempty" msgpack:"n,omitempty"`
	PreviousState string         `json:"previous_state" yaml:"ps,omitempty" msgpack:"ps,omitempty"`
	//Private           bool           `json:"private"`
	PullRequestNumber int64  `json:"pull_request_number" yaml:"prn,omitempty" msgpack:"prn,omitempty"`
	PullRequestTitle  string `json:"pull_request_title" yaml:"prt,omitempty" msgpack:"prt,omitempty"`
	/*
		Repository        struct {
			//Href        string `json:"@href" yaml:"-"`
			Permissions struct {
				Activate      bool `json:"activate"`
				Admin         bool `json:"admin"`
				CreateCron    bool `json:"create_cron"`
				CreateEnvVar  bool `json:"create_env_var"`
				CreateKeyPair bool `json:"create_key_pair"`
				CreateRequest bool `json:"create_request"`
				Deactivate    bool `json:"deactivate"`
				DeleteKeyPair bool `json:"delete_key_pair"`
				Migrate       bool `json:"migrate"`
				Read          bool `json:"read"`
				//Star          bool `json:"star" yaml:"-"`
				//Unstar        bool `json:"unstar" yaml:"-"`
			} `json:"@permissions"`
			//Representation string      `json:"@representation" yaml:"-"`
			//Type           string      `json:"@type" yaml:"-"`
			Active         bool        `json:"active"`
			ActiveOnOrg    interface{} `json:"active_on_org" yaml:",omitempty"`
			DefaultBranch  struct {
				//Href           string `json:"@href" yaml:"-"`
				//Representation string `json:"@representation" yaml:"-"`
				//Type           string `json:"@type" yaml:"-"`
				Name           string `json:"name"`
			} `json:"default_branch"`
			Description           string      `json:"description"`
			GithubID              int64         `json:"github_id"`
			GithubLanguage        string      `json:"github_language" yaml:",omitempty"`
			ID                    int64         `json:"id"`
			//ManagedByInstallation bool        `json:"managed_by_installation" yaml:"-"`
			//MigrationStatus       interface{} `json:"migration_status" yaml:"-"`
			//Name                  string      `json:"name" yaml:"-"`
			Owner                 struct {
				//Href  string `json:"@href" yaml:"-"`
				Type  string `json:"@type"`
				ID    int64    `json:"id"`
				Login string `json:"login"`
			} `json:"owner"`
			Private bool   `json:"private"`
			//Slug    string `json:"slug" yaml:"-"`
			//Starred bool   `json:"starred" yaml:"-"`
		 } `json:"repository" yaml:"-"` // TODO: get repository info from somewhere else.
	*/
	Request struct {
		//Href           string `json:"@href" yaml:"-"`
		//Representation string `json:"@representation" yaml:"-"`
		//Type           string `json:"@type" yaml:"-"`
		ID      int64  `json:"id" yaml:"i,omitempty" msgpack:"i,omitempty"`
		Message string `json:"message" yaml:"m,omitempty" msgpack:"m,omitempty"`
		Result  string `json:"result" yaml:"r,omitempty" msgpack:"r,omitempty"`
		State   string `json:"state" yaml:"s,omitempty" msgpack:"s,omitempty"`
	} `json:"request" yaml:"r,omitempty" msgpack:"r,omitempty"`
	//Stages    []interface{} `json:"stages" yaml:",omitempty"`
	StartedAt string `json:"started_at" yaml:"sa,omitempty" msgpack:"sa,omitempty"`
	State     string `json:"state" yaml:"st,omitempty" msgpack:"st,omitempty"`
	//Tag        interface{}   `json:"tag" yaml:"-"`
	UpdatedAt string `json:"updated_at" yaml:"ua,omitempty" msgpack:"ua,omitempty"`
}
