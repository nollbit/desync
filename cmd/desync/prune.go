package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/folbricht/desync"
	"github.com/spf13/cobra"
)

type pruneOptions struct {
	CmdStoreOptions
	store string
	yes   bool
}

func newPruneCommand(ctx context.Context) *cobra.Command {
	var opt pruneOptions

	cmd := &cobra.Command{
		Use:   "prune <index> [<file>..]",
		Short: "Remove unreferenced chunks from a store",
		Long: `Read chunk IDs in from index files and delete any chunks from a store
that are not referenced in the provided index files. Use '-' to read a single index
from STDIN.`,
		Example: `  desync prune -s /path/to/local --yes file.caibx`,
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPrune(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.store, "store", "s", "", "target store")
	flags.BoolVarP(&opt.yes, "yes", "y", false, "do not ask for confirmation")
	addStoreOptions(&opt.CmdStoreOptions, flags)
	return cmd
}

func runPrune(ctx context.Context, opt pruneOptions, args []string) error {
	if err := opt.CmdStoreOptions.validate(); err != nil {
		return err
	}
	if opt.store == "" {
		return errors.New("no store provided")
	}

	// Open the target store
	sr, err := storeFromLocation(opt.store, opt.CmdStoreOptions)
	if err != nil {
		return err
	}
	defer sr.Close()

	// Make sure this store can be used for pruning
	s, ok := sr.(desync.PruneStore)
	if !ok {
		return fmt.Errorf("store '%s' does not support pruning", opt.store)
	}

	// Read the input files and merge all chunk IDs in a map to de-dup them
	ids := make(map[desync.ChunkID]struct{})
	for _, name := range args {
		c, err := ReadCaibxFile(name, opt.CmdStoreOptions)
		if err != nil {
			return err
		}
		for _, c := range c.Chunks {
			ids[c.ID] = struct{}{}
		}
	}

	// If the -y option wasn't provided, ask the user to confirm before doing anything
	if !opt.yes {
		fmt.Printf("Warning: The provided index files reference %d unique chunks. Are you sure\nyou want to delete all other chunks from '%s'?\n", len(ids), s)
	ask:
		for {
			var a string
			fmt.Printf("[y/N]: ")
			fmt.Fscanln(os.Stdin, &a)
			switch a {
			case "y", "Y":
				break ask
			case "n", "N", "":
				return nil
			}
		}
	}

	return s.Prune(ctx, ids)
}
