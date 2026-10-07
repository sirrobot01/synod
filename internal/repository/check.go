package repository

import (
	"context"
	"errors"
	"math/rand/v2"

	"github.com/google/uuid"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
)

type CheckResult struct {
	Contents, Read, BytesRead int64
}

// Check reads and verifies a random sample of live content, with at least one read.
// Open a new repository connection for this check so a cache cannot mask storage damage.
func (r *Repository) Check(ctx context.Context, installationID uuid.UUID, percentage int) (CheckResult, error) {
	var result CheckResult
	if percentage < 1 || percentage > 100 {
		return result, &Error{Action: "check content", Cause: errors.New("invalid sample percentage")}
	}
	if err := r.CheckInstallation(ctx, installationID); err != nil {
		return result, err
	}
	direct, ok := r.repo.(repo.DirectRepository)
	if !ok {
		return result, &Error{Action: "check content", Cause: errors.New("direct repository required")}
	}
	reader := direct.ContentReader()
	err := reader.IterateContents(ctx, content.IterateOptions{Parallel: 1}, func(info content.Info) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		result.Contents++
		if result.Read > 0 && rand.IntN(100) >= percentage {
			return nil
		}
		data, err := reader.GetContent(ctx, info.ContentID)
		if err != nil {
			return err
		}
		result.Read++
		result.BytesRead += int64(len(data))
		return nil
	})
	if err == nil && result.Read == 0 {
		err = errors.New("repository has no live content")
	}
	if err != nil {
		return result, &Error{Action: "check content", Cause: errors.Join(err, ctx.Err())}
	}
	return result, nil
}
