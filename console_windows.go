package main

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode = kernel32.NewProc("SetConsoleMode")
)

const (
	ENABLE_QUICK_EDIT_MODE = 0x0040
    ENABLE_EXTENDED_FLAGS  = 0x0080
)

func disableQuickEdit() {
	handle, err := syscall.GetStdHandle(syscall.STD_INPUT_HANDLE)
	if err != nil {
        fmt.Println("[系统] 无法获取控制台句柄:", err)
		return
	}

	var mode uint32
	ret, _, err := procGetConsoleMode.Call(uintptr(handle), uintptr(unsafe.Pointer(&mode)))
	if ret == 0 {
         fmt.Println("[系统] 无法获取控制台模式:", err)
		return
	}

	// Disable Quick Edit Mode
    // Quick Edit causes the process to pause when user clicks inside the console window.
	newMode := mode &^ ENABLE_QUICK_EDIT_MODE
    newMode |= ENABLE_EXTENDED_FLAGS // Required when setting/clearing QuickEdit

	ret, _, err = procSetConsoleMode.Call(uintptr(handle), uintptr(newMode))
	if ret == 0 {
        fmt.Println("[系统] 无法设置控制台模式 (禁用快速编辑失败):", err)
		return
	}
    fmt.Println("[系统] 已禁用 Windows 快速编辑模式 (防止假死)")
}
