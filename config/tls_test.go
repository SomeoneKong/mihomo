package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTLSCustomCertificates(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		config string
		want   []string
	}{
		{"none", "tls: {}\n", nil},
		{"as documented", "tls:\n  custom-certifactes: [cert-a]\n", []string{"cert-a"}},
		{"correctly spelled", "tls:\n  custom-certificates: [cert-a]\n", []string{"cert-a"}},
		{"both", "tls:\n  custom-certifactes: [cert-a]\n  custom-certificates: [cert-b]\n", []string{"cert-a", "cert-b"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rawCfg, err := UnmarshalRawConfig([]byte(testCase.config))
			assert.NoError(t, err)
			tlsCfg, err := parseTLS(rawCfg)
			assert.NoError(t, err)
			assert.Equal(t, testCase.want, tlsCfg.CustomTrustCert)
		})
	}
}
