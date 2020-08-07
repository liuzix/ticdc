package framework

import (
	"github.com/pingcap/log"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvroKafkaDockerEnv_Basic(t *testing.T) {
	env := NewAvroKafkaDockerEnv()
	require.NotNil(t, env)

	env.Setup()

	bytes, err := env.ExecInController("echo test")
	require.NoErrorf(t, err, "Execution returned error", func() string {
		switch err.(type) {
		case *exec.ExitError:
			return string(err.(*exec.ExitError).Stderr)
		default:
			return ""
		}
	}())
	require.Equal(t, "test\n", string(bytes))

	env.TearDown()
}

type dummyTask struct {
	test *testing.T
}

func (t *dummyTask) Prepare(taskContext *TaskContext) error {
	return nil
}

func (t *dummyTask) GetCDCProfile() *CDCProfile {
	return &CDCProfile{
		PDUri:   "http://upstream-pd:2379",
		SinkUri: "kafka://kafka:9092/testdb_test?protocol=avro",
		Opts:    map[string]string{"registry": "http://schema-registry:8081"},
	}
}

func (t *dummyTask) Name() string {
	return "Dummy"
}

func (t *dummyTask) Run(taskContext *TaskContext) error {
	err := taskContext.Upstream.Ping()
	require.NoError(t.test, err, "Pinging upstream failed")

	err = taskContext.Downstream.Ping()
	require.NoError(t.test, err, "Pinging downstream failed")

	err = taskContext.CreateDB("testdb")
	require.NoError(t.test, err)

	log.Info("Running task")
	return nil
}

func TestAvroKafkaDockerEnv_RunTest(t *testing.T) {
	env := NewAvroKafkaDockerEnv()
	require.NotNil(t, env)

	env.Setup()
	env.RunTest(&dummyTask{test: t})
	env.TearDown()
}
