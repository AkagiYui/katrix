package csapi

import (
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// typingTracker wraps the sync package's TypingTracker for use by the API.
type typingTracker = syncpkg.TypingTracker

func newTypingTracker() *typingTracker {
	return syncpkg.NewTypingTracker(30 * time.Second)
}

// syncEngine wraps the sync package's Engine for use by the API.
type syncEngine = syncpkg.Engine

func newSyncEngine(store *storage.Store, typing *typingTracker) *syncEngine {
	return syncpkg.NewEngine(store, typing)
}
