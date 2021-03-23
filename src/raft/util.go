package raft

import "log"

// Debugging
const Verbose = true

func VPrintf(format string, a ...interface{}) (n int, err error) {
	if Verbose {
		log.Printf(format, a...)
	}
	return
}

type DummyLock struct {
}

func (d *DummyLock) Lock() {

}
func (d *DummyLock) Unlock() {

}
func (d *DummyLock) RLock() {

}
func (d *DummyLock) RUnlock() {

}
