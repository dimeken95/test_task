package buildinfo

// Set via -ldflags "-X github.com/dimeken95/test_task/internal/buildinfo.Version=..."
var (
	Version = "dev"
	Commit  = "none"
)
