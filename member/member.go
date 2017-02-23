package member


// MemberStatus is the state that a member is in.
type MemberStatus int

// Different possible states of serf member
const (
	StatusNone MemberStatus = iota
	StatusAlive
	StatusLeaving
	StatusLeft
	StatusFailed
	StatusReap
)

type ClusterMember struct {
	ID   int64  `json:"id"`
	IP   string `json:"addr"`

	RaftPort int          `json:"-"`
	Status   MemberStatus `json:"-"`
}
