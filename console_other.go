//go:build !windows

package main

func disableQuickEdit() {
    // Non-Windows systems do not need this
}
