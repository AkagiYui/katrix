package homeserver

// Version is the Katrix software version, reported over federation and in the
// server header. Overridden at build time via -ldflags on the cmd package and
// copied here at startup.
var Version = "0.1.0-dev"
