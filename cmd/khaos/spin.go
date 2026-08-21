package main

import (
	"fmt"
	"io"
	"time"
)

// spinFrames is a braille spinner: one cell wide, so the line never shifts as it turns.
var spinFrames = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

// spin runs fn while animating `⠋ doing… hint` on w, clearing the line when fn returns.
// The caller prints its own ✓ confirmation, since what there is to confirm (broker
// addresses, for one) usually only exists after fn succeeds.
//
// When styling is off -- piped output, NO_COLOR, CI -- it degrades to a single plain
// `doing…` line, so logs stay clean and carriage returns never reach a file.
func spin(w io.Writer, doing, hint string, fn func() error) error {
	styledHint := ""
	if hint != "" {
		styledHint = " " + styDim.Render(hint)
	}
	if !colorEnabled {
		fmt.Fprintf(w, "%s…%s\n", doing, styledHint)
		return fn()
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(90 * time.Millisecond)
		defer t.Stop()
		for i := 0; ; i++ {
			fmt.Fprintf(w, "\r\x1b[2K%s %s…%s",
				styTitle.Render(string(spinFrames[i%len(spinFrames)])), doing, styledHint)
			select {
			case <-stop:
				fmt.Fprint(w, "\r\x1b[2K")
				return
			case <-t.C:
			}
		}
	}()
	err := fn()
	close(stop)
	<-done
	if err != nil {
		fmt.Fprintf(w, "%s %s failed\n", styErr.Render("✗"), doing)
	}
	return err
}

// spinDone is the confirmation line that replaces a finished spinner.
func spinDone(w io.Writer, msg, detail string) {
	if detail != "" {
		detail = "  " + styDim.Render(detail)
	}
	fmt.Fprintf(w, "%s %s%s\n", styOK.Render("✓"), msg, detail)
}
