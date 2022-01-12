package migration

import "time"

type Direction rune

const (
	Down Direction = 'd'
	Up   Direction = 'u'
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

type Log struct {
	Migration
	Direction
	AppliedAt time.Time
}

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
