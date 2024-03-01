package main

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestParsePartitionsMetrics(t *testing.T) {
	// Read the input data from a file
	file, err := os.Open("test_data/sinfo_partition.txt")
	if err != nil {
		t.Fatalf("Can not open test data: %v", err)
	}
	sinfo, err := ioutil.ReadAll(file)
	file.Close()
	file, err = os.Open("test_data/squeue_partition.txt")
	if err != nil {
		t.Fatalf("Can not open test data: %v", err)
	}
	squeue, err := ioutil.ReadAll(file)
	file.Close()
	metrics := ParsePartitionsMetrics(string(sinfo), string(squeue))
	assert.Equal(t, 0., metrics["partition-1"].allocated)
	assert.Equal(t, 672., metrics["partition-1"].idle)
	assert.Equal(t, 2688., metrics["partition-1"].other)
	assert.Equal(t, 3360., metrics["partition-1"].total)
	assert.Equal(t, 0., metrics["partition-1"].resources)
	assert.Equal(t, 103., metrics["partition-2"].resources)
}
