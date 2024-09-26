package main

import (
	"context"
	"fmt"
	"github.com/integration-system/gds"
	"github.com/integration-system/gds/config"
	"github.com/integration-system/gds/jobs"
	"time"
)

type testStruct struct {
	Data string
}

// One node example
func main() {
	cfg := config.ClusterConfiguration{
		InMemory: true,
		// BootstrapCluster must be true only on one node in cluster.
		BootstrapCluster: true,
		OuterAddress:     "127.0.0.1:9673",
		// Peers contains all peers addresses
		Peers: []string{"127.0.0.1:9673"},
	}

	// 创建调度器
	scheduler, err := gds.NewScheduler(cfg)
	if err != nil {
		panic(err)
	}

	// newJob is used to restore type information between startups
	newJob := func() jobs.Job {
		// Fields in testStruct have to be public in order to unmarshal correctly
		return &jobs.OneTimeJob{Data: new(testStruct)}
	}
	jobType := "test_job"
	jobExecutor := func(job jobs.Job) error {
		oneTimeJob := job.(*jobs.OneTimeJob)
		payload := oneTimeJob.Data.(*testStruct)
		fmt.Printf("processed job with data %s\n", payload.Data)
		return nil
	}

	// Register job type
	//
	// 每个 jobTyp 关联一个 NewFn 和一个 ExecuteFn，其中 NewFn 用于创建 Typ 对应的专用 job 结构；ExecuteFn 用来执行具体逻辑；
	scheduler.RegisterExecutor(jobType, jobExecutor, newJob)

	// Wait until there will be leader in cluster. Until that all AddJob calls will fail
	//
	// [重要] 等待 raft leader 被选举出来
	scheduler.WaitCluster(context.Background())

	// 创建一个 "test_job" 类型的 job
	jobPayload := testStruct{Data: "1!"}
	// job keys must be unique across all job types
	key := "key-1"
	job, err := jobs.NewOneTimeJob(jobType, key, time.Now().Add(500*time.Millisecond), jobPayload)
	if err != nil {
		panic(err)
	}

	// 注册到 scheduler 中
	err = scheduler.AddJob(job)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second)
}
