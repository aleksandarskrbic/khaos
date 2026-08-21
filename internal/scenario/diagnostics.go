package scenario

import (
	"fmt"
	"strconv"
	"strings"
)

// Severity separates a fatal validation failure from an advisory one.
//
// Errors and warnings share one ordered list tagged by severity, which keeps source
// order across both kinds rather than requiring two parallel lists.
type Severity int

const (
	// SeverityError makes the scenario unusable; decoding is skipped.
	SeverityError Severity = iota
	// SeverityWarning is advisory. The scenario still loads and runs.
	SeverityWarning
)

// String renders the severity as it appears in Diagnostics.String.
func (s Severity) String() string {
	if s == SeverityWarning {
		return "warning"
	}
	return "error"
}

// Diagnostic is one validation finding.
//
// Path is the dotted/indexed location inside the scenario document, e.g.
// "topics[0].message_schema.fields[2].values". Line is the 1-based line in the source
// YAML, or 0 when the finding cannot be tied to a node.
type Diagnostic struct {
	Path     string
	Message  string
	Severity Severity
	Line     int
}

// String renders one finding as "severity: line N: path: message". The line part
// is omitted when Line is 0 and the path part when Path is empty.
func (d Diagnostic) String() string {
	var b strings.Builder
	b.WriteString(d.Severity.String())
	b.WriteString(": ")
	if d.Line > 0 {
		b.WriteString("line ")
		b.WriteString(strconv.Itoa(d.Line))
		b.WriteString(": ")
	}
	if d.Path != "" {
		b.WriteString(d.Path)
		b.WriteString(": ")
	}
	b.WriteString(d.Message)
	return b.String()
}

// Diagnostics is an ordered set of findings, in the order the validator walked the
// document.
type Diagnostics []Diagnostic

// HasErrors reports whether any finding has SeverityError.
func (d Diagnostics) HasErrors() bool {
	for _, diag := range d {
		if diag.Severity == SeverityError {
			return true
		}
	}
	return false
}

// Errors returns only the error-severity findings, in order.
func (d Diagnostics) Errors() Diagnostics { return d.filter(SeverityError) }

// Warnings returns only the warning-severity findings, in order.
func (d Diagnostics) Warnings() Diagnostics { return d.filter(SeverityWarning) }

func (d Diagnostics) filter(s Severity) Diagnostics {
	var out Diagnostics
	for _, diag := range d {
		if diag.Severity == s {
			out = append(out, diag)
		}
	}
	return out
}

// String renders every finding, one per line, errors first and then warnings rather
// than in interleaved discovery order.
func (d Diagnostics) String() string {
	if len(d) == 0 {
		return ""
	}
	lines := make([]string, 0, len(d))
	for _, diag := range d.Errors() {
		lines = append(lines, diag.String())
	}
	for _, diag := range d.Warnings() {
		lines = append(lines, diag.String())
	}
	return strings.Join(lines, "\n")
}

// Err returns an error carrying every error-severity finding, or nil when there are
// none. It exists so callers that only care about "did this load" can use the
// ordinary Go error path.
func (d Diagnostics) Err() error {
	errs := d.Errors()
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("scenario is invalid:\n%s", errs.String())
}
