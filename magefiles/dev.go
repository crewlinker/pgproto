// Package main describes automation tasks.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

func init() {
	mustBeInRootIfNotTest()
}

// Dev namespace holds development commands.
type Dev mg.Namespace

// Lint our codebase.
func (Dev) Lint() error {
	if err := sh.Run("golangci-lint", "run"); err != nil {
		return fmt.Errorf("failed to run golang-ci: %w", err)
	}

	return nil
}

// Test the whole codebase.
func (Dev) Test() error {
	if err := sh.Run("go", "test", "./..."); err != nil {
		return fmt.Errorf("failed to generate go code: %w", err)
	}

	return nil
}

// Serve will create or replace containers used for development.
func (Dev) Serve() error {
	if err := sh.RunWith(map[string]string{}, "docker", "compose",
		"-f", "docker-compose.yml",
		"up",
		"-d", "--build", "--remove-orphans", "--force-recreate",
	); err != nil {
		return fmt.Errorf("failed to run: %w", err)
	}

	return nil
}

func mustBeInRootIfNotTest() {
	if _, err := os.ReadFile("go.mod"); err != nil && !strings.Contains(strings.Join(os.Args, ""), "-test.") {
		panic("must be in project root, couldn't stat go.mod file: " + err.Error())
	}
}
