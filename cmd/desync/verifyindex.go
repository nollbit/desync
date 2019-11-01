package main

import (
	"context"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type verifyIndexOptions struct {
	desync.CmdStoreOptions
}

func newVerifyIndexCommand(ctx context.Context) *cobra.Command {
	var opt verifyIndexOptions

	cmd := &cobra.Command{
		Use:   "verify-index <index> <file>",
		Short: "Verifies an index matches a file",
		Long: `Verifies an index file matches the content of a blob. Use '-' to read the index
from STDIN.`,
		Example: `  desync verify-index sftp://192.168.1.1/myIndex.caibx largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVerifyIndex(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	desync.AddStoreOptions(&opt.CmdStoreOptions, flags)
	return cmd
}
func runVerifyIndex(ctx context.Context, opt verifyIndexOptions, args []string) error {
	if err := opt.CmdStoreOptions.Validate(); err != nil {
		return err
	}
	indexFile := args[0]
	dataFile := args[1]

	// Read the input
	idx, err := desync.ReadCaibxFile(indexFile, opt.CmdStoreOptions)
	if err != nil {
		return err
	}

	// If this is a terminal, we want a progress bar
	pb := NewProgressBar("")

	// Chop up the file into chunks and store them in the target store
	return desync.VerifyIndex(ctx, dataFile, idx, opt.N, pb)
}
