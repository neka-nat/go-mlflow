package mlflow

import "testing"

func TestGetExperiment(t *testing.T) {
	client := New("http://localhost:5000")
	t.Run("GetExperiment", func(t *testing.T) {
		t.Log("Test GetExperiment")
		experimentId, _ := client.CreateExperiment("test4")
		t.Log(experimentId)
		experiment, _ := client.GetExperiment(*experimentId)
		if experiment.ExperimentId != *experimentId {
			t.Errorf("Expected experiment id 1, got %s", experiment.ExperimentId)
		}
	})
}
