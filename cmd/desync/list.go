package main

import (
	"context"
	"fmt"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type listOptions struct {
	desync.CmdStoreOptions
}

func newListCommand(ctx context.Context) *cobra.Command {
	var opt listOptions

	cmd := &cobra.Command{
		Use:   "list-chunks <index>",
		Short: "List chunk IDs from an index",
		Long: `Reads the index file and prints the list of chunk IDs in it. Use '-' to read
the index from STDIN.`,
		Example: `  desync list-chunks file.caibx`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	desync.AddStoreOptions(&opt.CmdStoreOptions, flags)
	return cmd
}

func runList(ctx context.Context, opt listOptions, args []string) error {
	if err := opt.CmdStoreOptions.Validate(); err != nil {
		return err
	}

	// Read the input
	c, err := desync.ReadCaibxFile(args[0], opt.CmdStoreOptions)
	if err != nil {
		return err
	}
	// Write the list of chunk IDs to STDOUT
	for _, chunk := range c.Chunks {
		fmt.Fprintln(stdout, chunk.ID)
		// See if we're meant to stop
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}
