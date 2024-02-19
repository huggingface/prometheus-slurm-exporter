/* Copyright 2017 Victor Penso, Matteo Dessalvi

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
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type NodesPerPartitionMetrics struct {
	alloc map[string]float64
	comp  map[string]float64
	down  map[string]float64
	drain map[string]float64
	err   map[string]float64
	fail  map[string]float64
	idle  map[string]float64
	maint map[string]float64
	mix   map[string]float64
	resv  map[string]float64
}

func NewNodesPerPartitionMetrics() *NodesPerPartitionMetrics {
	return &NodesPerPartitionMetrics{
		alloc: make(map[string]float64),
		comp:  make(map[string]float64),
		down:  make(map[string]float64),
		drain: make(map[string]float64),
		err:   make(map[string]float64),
		fail:  make(map[string]float64),
		idle:  make(map[string]float64),
		maint: make(map[string]float64),
		mix:   make(map[string]float64),
		resv:  make(map[string]float64),
	}
}

func NodesPerPartitionGetMetrics() *NodesPerPartitionMetrics {
	return ParseNodesPerPartitionMetrics(NodesPerPartitionData())
}

func initPartitionMap(partition string, nm *NodesPerPartitionMetrics) {
	if _, ok := nm.alloc[partition]; !ok {
		nm.alloc[partition] = 0
		nm.comp[partition] = 0
		nm.down[partition] = 0
		nm.drain[partition] = 0
		nm.fail[partition] = 0
		nm.err[partition] = 0
		nm.idle[partition] = 0
		nm.maint[partition] = 0
		nm.mix[partition] = 0
		nm.resv[partition] = 0
	}
}

func ParseNodesPerPartitionMetrics(input []byte) *NodesPerPartitionMetrics {
	nm := NewNodesPerPartitionMetrics()
	lines := strings.Split(string(input), "\n")

	// Sort and remove all the duplicates from the 'sinfo' output
	sort.Strings(lines)
	lines_uniq := RemoveDuplicates(lines)

	for _, line := range lines_uniq {
		if strings.Contains(line, ",") {
			split := strings.Split(line, ",")
			count, _ := strconv.ParseFloat(strings.TrimSpace(split[0]), 64)
			state := split[1]
			partition := split[2]
			// remove * character from the partition
			partition = strings.TrimSuffix(partition, "*")
			alloc := regexp.MustCompile(`^alloc`)
			comp := regexp.MustCompile(`^comp`)
			down := regexp.MustCompile(`^down`)
			drain := regexp.MustCompile(`^drain`)
			fail := regexp.MustCompile(`^fail`)
			err := regexp.MustCompile(`^err`)
			idle := regexp.MustCompile(`^idle`)
			maint := regexp.MustCompile(`^maint`)
			mix := regexp.MustCompile(`^mix`)
			resv := regexp.MustCompile(`^res`)
			initPartitionMap(partition, nm)
			switch {
			case alloc.MatchString(state) == true:
				nm.alloc[partition] += count
			case comp.MatchString(state) == true:
				nm.comp[partition] += count
			case down.MatchString(state) == true:
				nm.down[partition] += count
			case drain.MatchString(state) == true:
				nm.drain[partition] += count
			case fail.MatchString(state) == true:
				nm.fail[partition] += count
			case err.MatchString(state) == true:
				nm.err[partition] += count
			case idle.MatchString(state) == true:
				nm.idle[partition] += count
			case maint.MatchString(state) == true:
				nm.maint[partition] += count
			case mix.MatchString(state) == true:
				nm.mix[partition] += count
			case resv.MatchString(state) == true:
				nm.resv[partition] += count
			}
		}
	}
	return nm
}

// Execute the sinfo command and return its output
func NodesPerPartitionData() []byte {
	cmd := exec.Command("sinfo", "-h", "-o %D,%T,%P")
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

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewNodesPerPartitionCollector() *NodesPerPartitionCollector {
	labels := []string{"partition"}
	return &NodesPerPartitionCollector{
		alloc: prometheus.NewDesc("slurm_nodes_alloc_per_partition", "Allocated nodes per partition", labels, nil),
		comp:  prometheus.NewDesc("slurm_nodes_comp_per_partition", "Completing nodes per partition", labels, nil),
		down:  prometheus.NewDesc("slurm_nodes_down_per_partition", "Down nodes per partition", labels, nil),
		drain: prometheus.NewDesc("slurm_nodes_drain_per_partition", "Drain nodes per partition", labels, nil),
		err:   prometheus.NewDesc("slurm_nodes_err_per_partition", "Error nodes per partition", labels, nil),
		fail:  prometheus.NewDesc("slurm_nodes_fail_per_partition", "Fail nodes per partition", labels, nil),
		idle:  prometheus.NewDesc("slurm_nodes_idle_per_partition", "Idle nodes per partition", labels, nil),
		maint: prometheus.NewDesc("slurm_nodes_maint_per_partition", "Maint nodes per partition", labels, nil),
		mix:   prometheus.NewDesc("slurm_nodes_mix_per_partition", "Mix nodes per partition", labels, nil),
		resv:  prometheus.NewDesc("slurm_nodes_resv_per_partition", "Reserved nodes per partition", labels, nil),
	}
}

type NodesPerPartitionCollector struct {
	alloc *prometheus.Desc
	comp  *prometheus.Desc
	down  *prometheus.Desc
	drain *prometheus.Desc
	err   *prometheus.Desc
	fail  *prometheus.Desc
	idle  *prometheus.Desc
	maint *prometheus.Desc
	mix   *prometheus.Desc
	resv  *prometheus.Desc
}

// Send all metric descriptions
func (nc *NodesPerPartitionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.alloc
	ch <- nc.comp
	ch <- nc.down
	ch <- nc.drain
	ch <- nc.err
	ch <- nc.fail
	ch <- nc.idle
	ch <- nc.maint
	ch <- nc.mix
	ch <- nc.resv
}
func (nc *NodesPerPartitionCollector) Collect(ch chan<- prometheus.Metric) {
	nm := NodesPerPartitionGetMetrics()
	for partition := range nm.alloc {
		ch <- prometheus.MustNewConstMetric(nc.alloc, prometheus.GaugeValue, nm.alloc[partition], partition)
	}
	for partition := range nm.comp {
		ch <- prometheus.MustNewConstMetric(nc.comp, prometheus.GaugeValue, nm.comp[partition], partition)
	}
	for partition := range nm.down {
		ch <- prometheus.MustNewConstMetric(nc.down, prometheus.GaugeValue, nm.down[partition], partition)
	}
	for partition := range nm.drain {
		ch <- prometheus.MustNewConstMetric(nc.drain, prometheus.GaugeValue, nm.drain[partition], partition)
	}
	for partition := range nm.err {
		ch <- prometheus.MustNewConstMetric(nc.err, prometheus.GaugeValue, nm.err[partition], partition)
	}
	for partition := range nm.fail {
		ch <- prometheus.MustNewConstMetric(nc.fail, prometheus.GaugeValue, nm.fail[partition], partition)
	}
	for partition := range nm.idle {
		ch <- prometheus.MustNewConstMetric(nc.idle, prometheus.GaugeValue, nm.idle[partition], partition)
	}
	for partition := range nm.maint {
		ch <- prometheus.MustNewConstMetric(nc.maint, prometheus.GaugeValue, nm.maint[partition], partition)
	}
	for partition := range nm.mix {
		ch <- prometheus.MustNewConstMetric(nc.mix, prometheus.GaugeValue, nm.mix[partition], partition)
	}
	for partition := range nm.resv {
		ch <- prometheus.MustNewConstMetric(nc.resv, prometheus.GaugeValue, nm.resv[partition], partition)
	}
}
