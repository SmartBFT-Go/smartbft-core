package types

import "testing"

// Pins the wire bytes of Proposal.Digest. These vectors are shared with the
// canonical module; a change here is a cluster-wide flag day, not a refactor.
func TestProposalDigestGolden(t *testing.T) {
	long := make([]byte, 128)
	for i := range long {
		long[i] = byte(i)
	}

	for _, tc := range []struct {
		name string
		p    Proposal
		want string
	}{
		{
			name: "empty",
			p:    Proposal{},
			want: "cc67164898e13d2ad50b32e740d8841aef5d0be6daefdf13492405ab087f793f",
		},
		{
			// Field order on the wire is the struct declaration order:
			// Payload, Header, Metadata, VerificationSequence.
			name: "reference",
			p: Proposal{
				Payload:              []byte("PAY"),
				Header:               []byte("HDR"),
				Metadata:             []byte("MET"),
				VerificationSequence: 7,
			},
			want: "35d84b08b379543f9d140ebc473d859c95e743dd0968adf54be28dd6e7727522",
		},
		{
			// 128 bytes crosses into DER long-form length encoding.
			name: "long-form-length",
			p:    Proposal{Payload: long},
			want: "0b91d84ecb42ce75e8e7cd900069b68db0dad026e389ef0af59737a2b3e553ed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.Digest(); got != tc.want {
				t.Fatalf("digest changed\n got: %s\nwant: %s\n\nthis breaks every stored proposal digest", got, tc.want)
			}
		})
	}
}
