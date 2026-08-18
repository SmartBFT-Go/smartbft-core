package bft

import (
	"encoding/hex"
	"testing"

	protos "github.com/SmartBFT-Go/smartbft-core/smartbftprotos"
)

// Pins CommitSignaturesDigest. Its output lands in ViewMetadata's
// PrevCommitSignatureDigest, so a change here is a cluster-wide flag day.
func TestCommitSignaturesDigestGolden(t *testing.T) {
	for _, tc := range []struct {
		name string
		sigs []*protos.Signature
		want string
	}{
		{
			name: "empty is nil",
			sigs: []*protos.Signature{},
			want: "",
		},
		{
			name: "single",
			sigs: []*protos.Signature{{Signer: 1, Value: []byte("V1"), Msg: []byte("M1")}},
			want: "376078b3b0ae99400a035f534579a528a42479773ab94a00b41f2c563edd9eb1",
		},
		{
			// Mixed nil payloads and a multi-byte signer id.
			name: "three",
			sigs: []*protos.Signature{
				{Signer: 1, Value: []byte("V1"), Msg: []byte("M1")},
				{Signer: 2, Value: nil, Msg: nil},
				{Signer: 300, Value: []byte("V3"), Msg: []byte("M3")},
			},
			want: "a31acfda5d67bf0c8bec72cb89f9b5abf3ea568fa6b2df6a9bb385e8baae376b",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := hex.EncodeToString(CommitSignaturesDigest(tc.sigs))
			if got != tc.want {
				t.Fatalf("digest changed\n got: %s\nwant: %s\n\nthis invalidates every stored PrevCommitSignatureDigest", got, tc.want)
			}
		})
	}
}
