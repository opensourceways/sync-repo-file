package server

import (
	"fmt"
	"strings"
)

type SyncFileConfig struct {
	// Platform is the code platform.
	Platform string `json:"platform" required:"true"`

	// Repos is either in the form of org/repos or just org.
	Repos []string `json:"repos" required:"true"`

	// ExcludedRepos is in the form of org/repo.
	ExcludedRepos []string `json:"excluded_repos" required:"true"`

	// FileNames is the list of files to be synchronized.
	FileNames []string `json:"file_names" required:"true"`
}

func (s SyncFileConfig) isExcluded(org, repo string) bool {
	return false
}

// the returned valude are org, repo.
func parseRepo(s string) (string, string) {
	s1 := strings.Trim(s, "/")
	if v := strings.Split(s1, "/"); len(v) == 2 {
		return v[0], v[1]
	}
	return s1, ""
}

func genRepoName(org, repo string) string {
	return fmt.Sprintf("%s/%s", org, repo)
}
