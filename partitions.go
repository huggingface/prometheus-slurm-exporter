/* Copyright 2020 Victor Penso

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

func PartitionsData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o%R,%C")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

func PartitionsPendingJobsData() []byte {
	cmd := exec.Command("squeue", "-a", "-r", "-h", "-o%P,%C,%r,%f", "--states=PENDING")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

type PartitionMetrics struct {
	allocated float64
	idle      float64
	other     float64
	pending   float64
	total     float64
	resources map[string]float64
}

func ParsePartitionsMetrics(partitionsData string, partitionsPendingJobsData string) map[string]*PartitionMetrics {
	partitions := make(map[string]*PartitionMetrics)
	lines := strings.Split(partitionsData, "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			// name of a partition
			partition := strings.Split(line, ",")[0]
			_, key := partitions[partition]
			if !key {
				partitions[partition] = &PartitionMetrics{0, 0, 0, 0, 0, map[string]float64{"null": 0}}
			}
			states := strings.Split(line, ",")[1]
			allocated, _ := strconv.ParseFloat(strings.Split(states, "/")[0], 64)
			idle, _ := strconv.ParseFloat(strings.Split(states, "/")[1], 64)
			other, _ := strconv.ParseFloat(strings.Split(states, "/")[2], 64)
			total, _ := strconv.ParseFloat(strings.Split(states, "/")[3], 64)
			partitions[partition].allocated = allocated
			partitions[partition].idle = idle
			partitions[partition].other = other
			partitions[partition].total = total
		}
	}
	// get list of pending jobs by partition name
	list := strings.Split(partitionsPendingJobsData, "\n")
	for _, line := range list {
		if strings.Contains(line, ",") {
			splits := strings.Split(line, ",")
			partition := strings.Split(line, ",")[0]
			requestedCpus, _ := strconv.ParseFloat(strings.Split(line, ",")[1], 64)
			reason := strings.Split(line, ",")[2]
			// features are a comma separated list enclosed in parentheses, convert to a list
			features := splits[3:] // everything after the first 3 columns is a feature (due to the split)
			features[0] = strings.Trim(features[0], "()")
			features[len(features)-1] = strings.Trim(features[len(features)-1], "()")
			// accumulate the number of pending jobs
			if _, key := partitions[partition]; key {
				partitions[partition].pending += 1
			}
			if reason == "Resources" {
				for _, feature := range features {
					if _, key := partitions[partition].resources[feature]; key {
						partitions[partition].resources[feature] += requestedCpus
					} else {
						partitions[partition].resources[feature] = requestedCpus
					}
				}
			}
		}
	}

	return partitions
}

type PartitionsCollector struct {
	allocated *prometheus.Desc
	idle      *prometheus.Desc
	other     *prometheus.Desc
	pending   *prometheus.Desc
	total     *prometheus.Desc
	resources *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
	labels := []string{"partition"}
	resourcesLabels := []string{"partition", "features"}
	return &PartitionsCollector{
		allocated: prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels, nil),
		idle:      prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels, nil),
		other:     prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels, nil),
		pending:   prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels, nil),
		total:     prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels, nil),
		resources: prometheus.NewDesc("slurm_partition_cpus_pending_resources", "Pending CPUs waiting for resources", resourcesLabels, nil),
	}
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- pc.allocated
	ch <- pc.idle
	ch <- pc.other
	ch <- pc.pending
	ch <- pc.total
	ch <- pc.resources
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
	pm := ParsePartitionsMetrics(string(PartitionsData()), string(PartitionsPendingJobsData()))
	for p := range pm {
		if pm[p].allocated > 0 {
			ch <- prometheus.MustNewConstMetric(pc.allocated, prometheus.GaugeValue, pm[p].allocated, p)
		}
		if pm[p].idle > 0 {
			ch <- prometheus.MustNewConstMetric(pc.idle, prometheus.GaugeValue, pm[p].idle, p)
		}
		if pm[p].other > 0 {
			ch <- prometheus.MustNewConstMetric(pc.other, prometheus.GaugeValue, pm[p].other, p)
		}
		if pm[p].pending > 0 {
			ch <- prometheus.MustNewConstMetric(pc.pending, prometheus.GaugeValue, pm[p].pending, p)
		}
		if pm[p].total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.total, prometheus.GaugeValue, pm[p].total, p)
		}
		for feature := range pm[p].resources {
			ch <- prometheus.MustNewConstMetric(pc.resources, prometheus.GaugeValue, pm[p].resources[feature], p, feature)
		}
	}
}
