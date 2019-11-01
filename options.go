package desync

import (
	"errors"

	"github.com/spf13/pflag"
)

// CmdStoreOptions are used to pass additional options to store initalization from the
// commandline. These generally override settings from the config file.
type CmdStoreOptions struct {
	N             int
	ClientCert    string
	ClientKey     string
	CaCert        string
	SkipVerify    bool
	TrustInsecure bool
}

// MergeWith takes store options as read from the config, and applies command-line
// provided options on top of them and returns the merged result.
func (o CmdStoreOptions) MergedWith(opt StoreOptions) StoreOptions {
	opt.N = o.N
	if o.ClientCert != "" {
		opt.ClientCert = o.ClientCert
	}
	if o.ClientKey != "" {
		opt.ClientKey = o.ClientKey
	}
	if o.CaCert != "" {
		opt.CACert = o.CaCert
	}
	if o.SkipVerify {
		opt.SkipVerify = true
	}
	if o.TrustInsecure {
		opt.TrustInsecure = true
	}
	return opt
}

// Validate the command line options are sensical and return an error if they aren't.
func (o CmdStoreOptions) Validate() error {
	if (o.ClientKey == "") != (o.ClientCert == "") {
		return errors.New("--client-key and --client-cert options need to be provided together")
	}
	return nil
}

// Add common store option flags to a command flagset.
func AddStoreOptions(o *CmdStoreOptions, f *pflag.FlagSet) {
	f.IntVarP(&o.N, "concurrency", "n", 10, "number of concurrent goroutines")
	f.StringVar(&o.ClientCert, "client-cert", "", "path to client certificate for TLS authentication")
	f.StringVar(&o.ClientKey, "client-key", "", "path to client key for TLS authentication")
	f.StringVar(&o.CaCert, "ca-cert", "", "trust authorities in this file, instead of OS trust store")
	f.BoolVarP(&o.TrustInsecure, "trust-insecure", "t", false, "trust invalid certificates")
}

// CmdServerOptions hold command line options used in HTTP servers.
type CmdServerOptions struct {
	Cert      string
	Key       string
	MutualTLS bool
	ClientCA  string
	Auth      string
}

func (o CmdServerOptions) Validate() error {
	if (o.Key == "") != (o.Cert == "") {
		return errors.New("--key and --cert options need to be provided together")
	}
	return nil
}

// Add common HTTP server options to a command flagset.
func AddServerOptions(o *CmdServerOptions, f *pflag.FlagSet) {
	f.StringVar(&o.Cert, "cert", "", "cert file in PEM format, requires --key")
	f.StringVar(&o.Key, "key", "", "key file in PEM format, requires --cert")
	f.BoolVar(&o.MutualTLS, "mutual-tls", false, "require valid client certficate")
	f.StringVar(&o.ClientCA, "client-ca", "", "acceptable client certificate or CA")
	f.StringVar(&o.Auth, "authorization", "", "expected value of the authorization header in requests")
}
