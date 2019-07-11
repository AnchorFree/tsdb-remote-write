package app

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	prom_labels "github.com/prometheus/prometheus/pkg/labels"
)

type SeriesState struct {
	Labels    prom_labels.Labels
	Timestamp int64
}

type SeriesSetState []SeriesState

type StateManager struct {
	stateFile string
	stateData SeriesSetState
	logger    log.Logger
	stateCh   chan SeriesState
	quit      chan struct{}
	wg        sync.WaitGroup
	ticker    time.Ticker
}

func NewStateManager(name string, saveInterval time.Duration, logger log.Logger) (*StateManager, error) {
	stateFile, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer stateFile.Close()

	fileContent, err := ioutil.ReadAll(stateFile)
	if err != nil {
		return nil, err
	}

	state := SeriesSetState{}
	_ = json.Unmarshal(fileContent, &state)

	level.Info(logger).Log("msg", "loaded saved state", "series", len(state))

	return &StateManager{
		stateFile: name,
		stateData: state,
		logger:    log.With(logger, "component", "state_manager"),
		ticker:    *time.NewTicker(saveInterval),
		stateCh:   make(chan SeriesState),
		quit:      make(chan struct{}),
		wg:        *new(sync.WaitGroup),
	}, nil
}

func (sm *StateManager) Start() {
	sm.saveHandler()
	sm.stateHandler()
}

func (sm *StateManager) stateHandler() {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		for {
			select {
			case seriesState := <-sm.stateCh:
				if seriesState.Timestamp > sm.GetTimestamp(seriesState.Labels) {
					sm.updateHandler(seriesState)
				}
			case <-sm.quit:
				return
			}
		}
	}()
	level.Info(sm.logger).Log("msg", "started state manager")
}

func (sm *StateManager) Update(labels prom_labels.Labels, ts int64) {
	sm.stateCh <- SeriesState{labels, ts}
}

func (sm *StateManager) Close() {
	sm.quit <- struct{}{}
	sm.wg.Wait()
	close(sm.stateCh)
	close(sm.quit)
}

func (sm *StateManager) GetTimestamp(labels prom_labels.Labels) int64 {
	for _, state := range sm.stateData {
		if prom_labels.Equal(state.Labels, labels) {
			return state.Timestamp
		}
	}
	return 0
}

func (sm *StateManager) updateHandler(seriesState SeriesState) {
	pos := -1
	for i, state := range sm.stateData {
		if prom_labels.Equal(state.Labels, seriesState.Labels) {
			pos = i
		}
	}
	if pos < 0 {
		sm.stateData = append(sm.stateData, seriesState)
		return
	}
	sm.stateData[pos] = seriesState
}

func (sm *StateManager) saveHandler() {
	sm.wg.Add(1)
	go func() {
		defer sm.wg.Done()
		for {
			select {
			case <-sm.ticker.C:
				sm.save()
			case <-sm.quit:
				sm.ticker.Stop()
				sm.save()
				return
			}
		}
	}()
	level.Info(sm.logger).Log("msg", "started save handler")
}

func (sm *StateManager) save() {
	b, _ := json.MarshalIndent(sm.stateData, "", "  ")
	if err := ioutil.WriteFile(sm.stateFile, b, 0644); err != nil {
		log.With(sm.logger, "msg", "failed to save state", "error", err)
	}
}
