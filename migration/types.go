package migration

import "time"

type Direction uint

const (
	Down Direction = 0
	Up   Direction = 1
)

// ---

const VersionBits = 64

type Version uint64

type Migration struct {
	Version Version
	Name    string
}

// ---

type Status uint

const (
	Pending Status = iota
	Applied
	Missing
)

// ---

type Description struct {
	Migration
	CanUndo bool
}

type State struct {
	Description
	Status    Status
	AppliedAt time.Time
}
