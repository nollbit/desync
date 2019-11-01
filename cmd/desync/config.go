package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func newConfigCommand(ctx context.Context) *cobra.Command {
	var write bool

	cmd := &cobra.Command{
		Use:   "config",
		Short: "Show or write config file",
		Long: `Shows the current internal configuration settings, either the defaults,
the values from $HOME/.config/desync/config.json or the specified config file. The
output can be used to create a custom config file writing it to the specified file
or $HOME/.config/desync/config.json by default.`,
		Example: `  desync config
  desync --config desync.json config -w`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runConfig(ctx, write)
		},
		SilenceUsage: true,
	}

	flags := cmd.Flags()
	flags.BoolVarP(&write, "write", "w", false, "write current configuration to file")
	return cmd
}

func runConfig(ctx context.Context, write bool) error {
	b, err := json.MarshalIndent(desync.Cfg, "", "  ")
	if err != nil {
		return err
	}
	var w io.Writer = os.Stderr
	if write {
		if err = os.MkdirAll(filepath.Dir(cfgFile), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(cfgFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		defer f.Close()
		fmt.Println("Writing config to", cfgFile)
		w = f
	}
	_, err = w.Write(b)
	fmt.Println()
	return err
}

// Global config in the main packe defining the defaults. Those can be
// overridden by loading a config file or in the command line.
var cfgFile string

// Look for $HOME/.config/desync and if present, load into the global config
// instance. Values defined in the file will be set accordingly, while anything
// that's not in the file will retain it's default values.
func initConfig() {
	var defaultLocation bool
	if cfgFile == "" {
		switch runtime.GOOS {
		case "windows":
			cfgFile = filepath.Join(os.Getenv("HOMEDRIVE")+os.Getenv("HOMEPATH"), ".config", "desync", "config.json")
		default:
			cfgFile = filepath.Join(os.Getenv("HOME"), ".config", "desync", "config.json")
		}
		defaultLocation = true
	}
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		if defaultLocation { // no problem if the default config doesn't exist
			return
		}
		die(err)
	}
	f, err := os.Open(cfgFile)
	if err != nil {
		die(err)
	}
	defer f.Close()
	if err = json.NewDecoder(f).Decode(&desync.Cfg); err != nil {
		die(errors.Wrap(err, "reading "+cfgFile))
	}
}

// Digest algorithm to be used by desync globally.
var digestAlgorithm string

func setDigestAlgorithm() {
	switch digestAlgorithm {
	case "", "sha512-256":
		desync.Digest = desync.SHA512256{}
	case "sha256":
		desync.Digest = desync.SHA256{}
	default:
		die(fmt.Errorf("invalid digest algorithm '%s'", digestAlgorithm))
	}
}
