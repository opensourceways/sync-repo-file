package client

import (
	"context"
	"time"

	grpc "github.com/opensourceways/sync-file-server/grpc/client"
	"github.com/opensourceways/sync-file-server/protocol"

	"github.com/opensourceways/sync-repo-file/server"
)

func NewSyncFileClient(endpoint string) (*SyncFileClient, error) {
	c, err := grpc.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	return &SyncFileClient{c: c}, nil
}

type SyncFileClient struct {
	c grpc.Client
}

func (s *SyncFileClient) ListRepos(org string) ([]string, error) {
	v, err := s.c.ListRepos(context.Background(), &protocol.ListRepoRequest{Org: org})
	if err != nil {
		return nil, err
	}

	return v.GetRepos(), nil
}

func (s *SyncFileClient) ListBranchOfRepo(org, repo string) ([]server.BranchInfo, error) {
	v, err := s.c.ListBranchesOfRepo(
		context.Background(),
		&protocol.ListBranchesOfRepoRequest{Org: org, Repo: repo},
	)
	if err != nil {
		return nil, err
	}

	d := v.GetBranches()
	n := len(d)
	if n == 0 {
		return nil, nil
	}

	r := make([]server.BranchInfo, n)
	for i := 0; i < n; i++ {
		r[i] = server.BranchInfo{
			Name: d[i].GetName(),
			SHA:  d[i].GetSha(),
		}
	}
	return r, nil
}

func (s *SyncFileClient) SyncFileOfBranch(org, repo, branch, branchSHA string, files []string) error {
	req := protocol.SyncRepoFileRequest{
		Branch: &protocol.Branch{
			Org:       org,
			Repo:      repo,
			Branch:    branch,
			BranchSha: branchSHA,
		},
		FileNames: files,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := s.c.SyncRepoFile(ctx, &req)
	return err
}

func (s *SyncFileClient) Stop() error {
	if s != nil && s.c != nil {
		return s.c.Disconnect()
	}
	return nil
}
