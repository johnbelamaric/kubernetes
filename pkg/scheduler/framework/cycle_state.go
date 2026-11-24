/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
)

type cycleStateEntry struct {
	key fwk.StateKey
	val fwk.StateData
}

// Note: CycleState uses an atomic.Pointer to a copy-on-write slice backed by writeLock, because it is thread safe
// and optimizes for the "write once in PreFilter/PreScore and read many times in Filter/Score" pattern.
type CycleState struct {
	// storage holds copy-on-write StateKey -> StateData entries for zero-alloc lock-free Reads.
	storage atomic.Pointer[[]cycleStateEntry]
	// writeLock serializes Write, Delete, and bulk copy operations on storage.
	writeLock sync.Mutex
	// if recordPluginMetrics is true, metrics.PluginExecutionDuration will be recorded for this cycle.
	recordPluginMetrics bool
	// skipFilterPlugins are plugins that will be skipped in the Filter extension point.
	skipFilterPlugins sets.Set[string]
	// skipFilterMask caches the bitmask of skipped Filter plugins (when <= 64 plugins).
	skipFilterMask    uint64
	hasSkipFilterMask bool
	// skipScorePlugins are plugins that will be skipped in the Score extension point.
	skipScorePlugins sets.Set[string]
	// skipScoreMask caches the bitmask of skipped Score plugins (when <= 64 plugins).
	skipScoreMask    uint64
	hasSkipScoreMask bool
	// skipPreBindPlugins are plugins that will be skipped in the PreBind extension point.
	skipPreBindPlugins sets.Set[string]
	// skipAllPostFilterPlugins indicates whether to skip all plugins in the PostFilter extension point.
	skipAllPostFilterPlugins bool
	// GetParallelPreBindPlugins returns plugins that can be run in parallel with other plugins
	// in the PreBind extension point.
	parallelPreBindPlugins sets.Set[string]
	// podGroupCycleState contains the CycleState for this pod's PodGroup.
	// If set to nil, it means that the pod referencing this CycleState either passed the pod group cycle
	// or doesn't belong to any pod group.
	// This field can only be non-nil when GenericWorkload feature flag is enabled.
	podGroupCycleState fwk.PodGroupCycleState
	// placementCycleState contains the CycleState for the current Placement being evaluated.
	// If set to nil, it means this pod is not being scheduled within a placement context.
	// This field can only be non-nil when GenericWorkload feature flag is enabled.
	placementCycleState fwk.PlacementCycleState
	// placementStates holds per-placement state keyed by Placement pointer.
	// PlacementGeneratePlugins populate it via PlacementState during placement generation.
	// The framework merges entries across plugins and copies the state for a placement
	// into the per-placement CycleState before placement evaluation.
	// Only populated on a PodGroup-level CycleState.
	placementStates map[*fwk.Placement]*CycleState
	// placementStatesLock guards placementStates.
	placementStatesLock sync.Mutex
}

// NewCycleState initializes a new CycleState and returns its pointer.
func NewCycleState() *CycleState {
	return &CycleState{}
}

// ShouldRecordPluginMetrics returns whether metrics.PluginExecutionDuration metrics should be recorded.
func (c *CycleState) ShouldRecordPluginMetrics() bool {
	if c == nil {
		return false
	}
	return c.recordPluginMetrics
}

// SetRecordPluginMetrics sets recordPluginMetrics to the given value.
func (c *CycleState) SetRecordPluginMetrics(flag bool) {
	if c == nil {
		return
	}
	c.recordPluginMetrics = flag
}

func (c *CycleState) SetSkipFilterPlugins(plugins sets.Set[string]) {
	c.skipFilterPlugins = plugins
	c.skipFilterMask = 0
	c.hasSkipFilterMask = false
}

func (c *CycleState) GetSkipFilterPlugins() sets.Set[string] {
	return c.skipFilterPlugins
}

// SetSkipFilterMask records a precomputed bitmask matching skipFilterPlugins.
func (c *CycleState) SetSkipFilterMask(mask uint64) {
	c.skipFilterMask = mask
	c.hasSkipFilterMask = true
}

// GetSkipFilterMask returns the cached filter skip bitmask if valid.
func (c *CycleState) GetSkipFilterMask() (uint64, bool) {
	return c.skipFilterMask, c.hasSkipFilterMask
}

func (c *CycleState) SetSkipScorePlugins(plugins sets.Set[string]) {
	c.skipScorePlugins = plugins
	c.skipScoreMask = 0
	c.hasSkipScoreMask = false
}

func (c *CycleState) GetSkipScorePlugins() sets.Set[string] {
	return c.skipScorePlugins
}

// SetSkipScoreMask records a precomputed bitmask matching skipScorePlugins.
func (c *CycleState) SetSkipScoreMask(mask uint64) {
	c.skipScoreMask = mask
	c.hasSkipScoreMask = true
}

// GetSkipScoreMask returns the cached score skip bitmask if valid.
func (c *CycleState) GetSkipScoreMask() (uint64, bool) {
	return c.skipScoreMask, c.hasSkipScoreMask
}

func (c *CycleState) SetSkipPreBindPlugins(plugins sets.Set[string]) {
	c.skipPreBindPlugins = plugins
}

func (c *CycleState) GetSkipPreBindPlugins() sets.Set[string] {
	return c.skipPreBindPlugins
}

func (c *CycleState) SetParallelPreBindPlugins(plugins sets.Set[string]) {
	c.parallelPreBindPlugins = plugins
}

func (c *CycleState) GetParallelPreBindPlugins() sets.Set[string] {
	return c.parallelPreBindPlugins
}

func (c *CycleState) IsPodGroupSchedulingCycle() bool {
	return c.podGroupCycleState != nil
}

func (c *CycleState) SetPodGroupCycleState(podGroupCycleState fwk.PodGroupCycleState) {
	c.podGroupCycleState = podGroupCycleState
}

func (c *CycleState) GetPodGroupCycleState() fwk.PodGroupCycleState {
	return c.podGroupCycleState
}

func (c *CycleState) GetPlacementCycleState() fwk.PlacementCycleState {
	return c.placementCycleState
}

// GetParentPlacementCycleState implements [fwk.PodGroupCycleState.GetParentPlacementCycleState].
// We can reuse the same placementCycleState field as GetPlacementCycleState, as it implements a different interface
// and the two are mutually exclusive.
func (c *CycleState) GetParentPlacementCycleState() fwk.PlacementCycleState {
	return c.placementCycleState
}

func (c *CycleState) SetPlacementCycleState(placementCycleState fwk.PlacementCycleState) {
	c.placementCycleState = placementCycleState
}

// PlacementState returns the PlacementCycleState for the given placement.
// If no state exists for the placement, the method initializes and returns a new one.
// If placement is nil, the method returns nil.
func (c *CycleState) PlacementState(placement *fwk.Placement) fwk.PlacementCycleState {
	if c == nil || placement == nil {
		return nil
	}
	c.placementStatesLock.Lock()
	defer c.placementStatesLock.Unlock()
	if c.placementStates == nil {
		c.placementStates = make(map[*fwk.Placement]*CycleState)
	}
	ps := c.placementStates[placement]
	if ps == nil {
		ps = NewCycleState()
		c.placementStates[placement] = ps
	}
	return ps
}

// DeletePlacementStates removes the PlacementCycleState entries for the given placements.
// The framework calls this method after merging placements to remove intermediate parent states.
func (c *CycleState) DeletePlacementStates(placements ...*fwk.Placement) {
	if c == nil {
		return
	}
	c.placementStatesLock.Lock()
	defer c.placementStatesLock.Unlock()
	if len(c.placementStates) == 0 {
		return
	}
	for _, p := range placements {
		delete(c.placementStates, p)
	}
}

// CopyPlacementDataInto clones every StateData entry registered for placement and writes
// it into dst. The scheduler calls this method to seed a per-placement CycleState before
// placement evaluation.
func (c *CycleState) CopyPlacementDataInto(placement *fwk.Placement, dst *CycleState) {
	if c == nil || placement == nil || dst == nil {
		return
	}
	c.placementStatesLock.Lock()
	src := c.placementStates[placement]
	c.placementStatesLock.Unlock()
	if src == nil {
		return
	}
	srcPtr := src.storage.Load()
	if srcPtr == nil || len(*srcPtr) == 0 {
		return
	}
	srcEntries := *srcPtr
	dst.writeLock.Lock()
	defer dst.writeLock.Unlock()
	var oldEntries []cycleStateEntry
	if dstPtr := dst.storage.Load(); dstPtr != nil {
		oldEntries = *dstPtr
	}
	next := make([]cycleStateEntry, len(oldEntries), len(oldEntries)+len(srcEntries))
	copy(next, oldEntries)
	for i := range srcEntries {
		k := srcEntries[i].key
		v := srcEntries[i].val.Clone()
		replaced := false
		for j := range next {
			if next[j].key == k {
				next[j].val = v
				replaced = true
				break
			}
		}
		if !replaced {
			next = append(next, cycleStateEntry{key: k, val: v})
		}
	}
	dst.storage.Store(&next)
}

// MergePlacementStatesInto combines the placement states for srcs into a single
// PlacementCycleState for dst. The method clones each StateData entry so the merged
// placement does not share mutable state with the source placements. If no source
// holds state, the method leaves dst unregistered and allocates nothing on the heap.
// If two source states contain the same StateKey, the method returns an error.
func (c *CycleState) MergePlacementStatesInto(dst *fwk.Placement, srcs ...*fwk.Placement) error {
	if c == nil || dst == nil {
		return nil
	}
	c.placementStatesLock.Lock()
	defer c.placementStatesLock.Unlock()
	if len(c.placementStates) == 0 {
		return nil
	}

	var mergedEntries []cycleStateEntry
	for _, srcPlacement := range srcs {
		src := c.placementStates[srcPlacement]
		if src == nil {
			continue
		}
		srcPtr := src.storage.Load()
		if srcPtr == nil || len(*srcPtr) == 0 {
			continue
		}
		for _, entry := range *srcPtr {
			for i := range mergedEntries {
				if mergedEntries[i].key == entry.key {
					return fmt.Errorf("conflicting placement cycle state key %q while merging into placement %q", entry.key, dst.Name)
				}
			}
			mergedEntries = append(mergedEntries, cycleStateEntry{
				key: entry.key,
				val: entry.val.Clone(),
			})
		}
	}
	if len(mergedEntries) == 0 {
		return nil
	}
	merged := NewCycleState()
	merged.storage.Store(&mergedEntries)
	c.placementStates[dst] = merged
	return nil
}

func (c *CycleState) SetSkipAllPostFilterPlugins(flag bool) {
	c.skipAllPostFilterPlugins = flag
}

func (c *CycleState) ShouldSkipAllPostFilterPlugins() bool {
	return c.skipAllPostFilterPlugins
}

// Clone creates a copy of CycleState and returns its pointer. Clone returns
// nil if the context being cloned is nil.
func (c *CycleState) Clone() fwk.CycleState {
	if c == nil {
		return nil
	}
	copied := NewCycleState()
	// Safe copy storage in case of overwriting.
	if p := c.storage.Load(); p != nil && len(*p) > 0 {
		src := *p
		cloned := make([]cycleStateEntry, len(src))
		for i := range src {
			cloned[i] = cycleStateEntry{
				key: src[i].key,
				val: src[i].val.Clone(),
			}
		}
		copied.storage.Store(&cloned)
	}
	// The below are not mutated, so we don't have to safe copy.
	copied.recordPluginMetrics = c.recordPluginMetrics
	copied.skipFilterPlugins = c.skipFilterPlugins
	copied.skipFilterMask = c.skipFilterMask
	copied.hasSkipFilterMask = c.hasSkipFilterMask
	copied.skipScorePlugins = c.skipScorePlugins
	copied.skipScoreMask = c.skipScoreMask
	copied.hasSkipScoreMask = c.hasSkipScoreMask
	copied.skipPreBindPlugins = c.skipPreBindPlugins
	copied.parallelPreBindPlugins = c.parallelPreBindPlugins
	copied.podGroupCycleState = c.podGroupCycleState
	copied.placementCycleState = c.placementCycleState
	copied.skipAllPostFilterPlugins = c.skipAllPostFilterPlugins

	// Deep copy the per-placement states so the clone does not share mutable StateData.
	c.placementStatesLock.Lock()
	if len(c.placementStates) > 0 {
		copied.placementStates = make(map[*fwk.Placement]*CycleState, len(c.placementStates))
		for p, st := range c.placementStates {
			if st == nil {
				continue
			}
			dup := NewCycleState()
			if stPtr := st.storage.Load(); stPtr != nil && len(*stPtr) > 0 {
				src := *stPtr
				cloned := make([]cycleStateEntry, len(src))
				for i := range src {
					cloned[i] = cycleStateEntry{
						key: src[i].key,
						val: src[i].val.Clone(),
					}
				}
				dup.storage.Store(&cloned)
			}
			copied.placementStates[p] = dup
		}
	}
	c.placementStatesLock.Unlock()

	return copied
}

// Read retrieves data with the given "key" from CycleState. If the key is not
// present, ErrNotFound is returned.
//
// See CycleState for notes on concurrency.
func (c *CycleState) Read(key fwk.StateKey) (fwk.StateData, error) {
	p := c.storage.Load()
	if p == nil {
		return nil, fwk.ErrNotFound
	}
	entries := *p
	for i := range entries {
		if entries[i].key == key {
			return entries[i].val, nil
		}
	}
	return nil, fwk.ErrNotFound
}

// Write stores the given "val" in CycleState with the given "key".
//
// See CycleState for notes on concurrency.
func (c *CycleState) Write(key fwk.StateKey, val fwk.StateData) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	var oldEntries []cycleStateEntry
	if p := c.storage.Load(); p != nil {
		oldEntries = *p
	}
	for i := range oldEntries {
		if oldEntries[i].key == key {
			next := make([]cycleStateEntry, len(oldEntries))
			copy(next, oldEntries)
			next[i].val = val
			c.storage.Store(&next)
			return
		}
	}
	next := make([]cycleStateEntry, len(oldEntries)+1)
	copy(next, oldEntries)
	next[len(oldEntries)] = cycleStateEntry{key: key, val: val}
	c.storage.Store(&next)
}

// Delete deletes data with the given key from CycleState.
//
// See CycleState for notes on concurrency.
func (c *CycleState) Delete(key fwk.StateKey) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	p := c.storage.Load()
	if p == nil {
		return
	}
	oldEntries := *p
	idx := -1
	for i := range oldEntries {
		if oldEntries[i].key == key {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	if len(oldEntries) == 1 {
		c.storage.Store(nil)
		return
	}
	next := make([]cycleStateEntry, len(oldEntries)-1)
	copy(next, oldEntries[:idx])
	copy(next[idx:], oldEntries[idx+1:])
	c.storage.Store(&next)
}
