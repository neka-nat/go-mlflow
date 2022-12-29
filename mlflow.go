package mlflow

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Client struct {
	// HTTP client used to communicate with the API.
	Client  *http.Client
	BaseUrl string
}

type ResponseExperiment struct {
	Experiment Experiment `json:"experiment"`
}

type Experiment struct {
	ExperimentId     string `json:"experiment_id"`
	Name             string `json:"name"`
	ArtifactLocation string `json:"artifact_location"`
	LifecycleStage   string `json:"lifecycle_stage"`
}

type ResponseCreateExperiment struct {
	ExperimentId string `json:"experiment_id"`
}

type ResponseRun struct {
	Run Run `json:"run"`
}

type Run struct {
	Info RunInfo                `json:"info"`
	Data map[string]interface{} `json:"data"`
}

type RunInfo struct {
	RunUUid        string `json:"run_uuid"`
	ExperimentId   string `json:"experiment_id"`
	UserId         string `json:"user_id"`
	Status         string `json:"status"`
	StartTime      int64  `json:"start_time"`
	EndTime        int64  `json:"end_time,omitempty"`
	ArtifactUri    string `json:"artifact_uri"`
	LifecycleStage string `json:"lifecycle_stage"`
	RunId          string `json:"run_id"`
}

type ResponseRunUpdate struct {
	Info RunInfo `json:"run_info"`
}

type RunStatus string

const (
	Running       RunStatus = "RUNNING"
	Scheduled     RunStatus = "SCHEDULED"
	Finished      RunStatus = "FINISHED"
	Failed        RunStatus = "FAILED"
	Killed        RunStatus = "KILLED"
	Uninitialized RunStatus = "UNINITIALIZED"
)

func AddQuery(q url.Values, key string, value interface{}) {
	switch value := value.(type) {
	case string:
		q.Add(key, value)
	case int:
		q.Add(key, strconv.Itoa(value))
	case int64:
		q.Add(key, strconv.FormatInt(value, 10))
	case bool:
		q.Add(key, strconv.FormatBool(value))
	case []string:
		for _, v := range value {
			q.Add(key, v)
		}
	case []interface{}:
		for _, v := range value {
			AddQuery(q, key, v)
		}
	case map[string]interface{}:
		for k, v := range value {
			AddQuery(q, key+"."+k, v)
		}
	}
}

func New(url string) *Client {
	return &Client{
		Client:  http.DefaultClient,
		BaseUrl: url,
	}
}

func (p *Client) HandleGet(url string, params map[string]interface{}) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	for key, value := range params {
		AddQuery(q, key, value)
	}
	req.URL.RawQuery = q.Encode()
	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return body, nil
	}
	return nil, nil
}

func (p *Client) HandlePost(url string, request interface{}) ([]byte, error) {
	b, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusOK {
		return body, nil
	}
	return nil, nil
}

func (p *Client) GetExperiment(experimentId string) (*Experiment, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/experiments/get"
	body, err := p.HandleGet(url, map[string]interface{}{"experiment_id": experimentId})
	if err != nil {
		return nil, err
	}
	var response ResponseExperiment
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.Experiment, nil
}

func (p *Client) GetExperimentsByName(name string) (*Experiment, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/experiments/get-by-name"
	body, err := p.HandleGet(url, map[string]interface{}{"experiment_name": name})
	if err != nil {
		return nil, err
	}
	var response ResponseExperiment
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.Experiment, nil
}

func (p *Client) CreateExperiment(name string) (*string, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/experiments/create"
	body, err := p.HandlePost(url, map[string]interface{}{"name": name})
	if err != nil {
		return nil, err
	}
	var response ResponseCreateExperiment
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.ExperimentId, nil
}

func (p *Client) CreateRunWithStartTime(experimentId string, startTime int64, tags []map[string]string) (*Run, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/runs/create"
	body, err := p.HandlePost(url, map[string]interface{}{"experiment_id": experimentId, "start_time": startTime, "tags": tags})
	if err != nil {
		return nil, err
	}
	var response ResponseRun
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.Run, nil
}

func (p *Client) CreateRun(experimentId string, tags []map[string]string) (*Run, error) {
	return p.CreateRunWithStartTime(experimentId, time.Now().Unix(), tags)
}

func (p *Client) UpdateRunWithEndTime(runId string, status RunStatus, endTime int64) (*RunInfo, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/runs/update"
	body, err := p.HandlePost(url, map[string]interface{}{"run_id": runId, "status": status, "end_time": endTime})
	if err != nil {
		return nil, err
	}
	var response ResponseRunUpdate
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.Info, nil
}

func (p *Client) UpdateRun(runId string, status RunStatus) (*RunInfo, error) {
	return p.UpdateRunWithEndTime(runId, status, time.Now().Unix())
}

func (p *Client) DeleteRun(runId string) error {
	url := p.BaseUrl + "/api/2.0/mlflow/runs/delete"
	_, err := p.HandlePost(url, map[string]interface{}{"run_id": runId})
	return err
}

func (p *Client) GetRun(runId string) (*Run, error) {
	url := p.BaseUrl + "/api/2.0/mlflow/runs/get"
	body, err := p.HandleGet(url, map[string]interface{}{"run_id": runId})
	if err != nil {
		return nil, err
	}
	var response ResponseRun
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}
	return &response.Run, nil
}
