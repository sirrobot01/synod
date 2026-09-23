package cluster

// ListOptions selects one bounded page of installation clusters.
type ListOptions struct {
	Limit          int
	Cursor         string
	IncludeDeleted bool
}

// Page uses an empty NextCursor when no further page is available.
type Page struct {
	Items      []Cluster `json:"items"`
	NextCursor string    `json:"next_cursor"`
}
