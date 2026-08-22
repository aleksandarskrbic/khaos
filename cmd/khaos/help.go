package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

// setHelpStyle propagates to every subcommand: cobra falls back to the nearest ancestor's
// helpFunc/usageFunc when a command has none of its own.
func setHelpStyle(root *cobra.Command) {
	root.SetHelpFunc(styledHelp)
	root.SetUsageFunc(styledUsage)
}

func styledHelp(c *cobra.Command, _ []string) {
	w := c.OutOrStdout()
	fmt.Fprintln(w, styTitle.Render(c.CommandPath()))

	desc := strings.TrimRight(c.Long, "\n")
	if desc == "" {
		desc = c.Short
	}
	if desc != "" {
		fmt.Fprintln(w)
		fmt.Fprintln(w, desc)
	}

	if c.Runnable() || c.HasSubCommands() {
		fmt.Fprintln(w)
		fmt.Fprint(w, styledUsageString(c))
	}
}

func styledUsage(c *cobra.Command) error {
	fmt.Fprint(c.OutOrStderr(), styledUsageString(c))
	return nil
}

func styledUsageString(c *cobra.Command) string {
	var b strings.Builder

	fmt.Fprint(&b, styHeader.Render("Usage:"))
	if c.Runnable() {
		fmt.Fprintf(&b, "\n  %s", c.UseLine())
	}
	if c.HasAvailableSubCommands() {
		fmt.Fprintf(&b, "\n  %s [command]", c.CommandPath())
	}

	if len(c.Aliases) > 0 {
		fmt.Fprintf(&b, "\n\n%s\n  %s", styHeader.Render("Aliases:"), c.NameAndAliases())
	}

	if c.HasExample() {
		fmt.Fprintf(&b, "\n\n%s\n%s", styHeader.Render("Examples:"), c.Example)
	}

	if c.HasAvailableSubCommands() {
		fmt.Fprintf(&b, "\n\n%s\n%s", styHeader.Render("Available Commands:"), commandTable(c))
	}

	if c.HasAvailableLocalFlags() {
		fmt.Fprintf(&b, "\n\n%s\n%s", styHeader.Render("Flags:"), colorizeFlagNames(c.LocalFlags().FlagUsages()))
	}

	if c.HasAvailableInheritedFlags() {
		fmt.Fprintf(&b, "\n\n%s\n%s", styHeader.Render("Global Flags:"), colorizeFlagNames(c.InheritedFlags().FlagUsages()))
	}

	if c.HasHelpSubCommands() {
		fmt.Fprintf(&b, "\n\n%s", styHeader.Render("Additional help topics:"))
		for _, sub := range c.Commands() {
			if sub.IsAdditionalHelpTopicCommand() {
				fmt.Fprintf(&b, "\n  %s %s",
					styScenario.Render(rpad(sub.CommandPath(), sub.CommandPathPadding())), sub.Short)
			}
		}
	}

	if c.HasAvailableSubCommands() {
		fmt.Fprintf(&b, "\n\nUse \"%s [command] --help\" for more information about a command.", c.CommandPath())
	}

	fmt.Fprintln(&b)
	return b.String()
}

func commandTable(c *cobra.Command) string {
	t := newListTable("COMMAND", "DESCRIPTION")
	for _, sub := range c.Commands() {
		if sub.IsAvailableCommand() || sub.Name() == "help" {
			t.Row(styScenario.Render(sub.Name()), sub.Short)
		}
	}
	return t.String()
}

// pflag right-pads every flag spec in a set to the same width, so the gap before the
// description is always 2+ spaces, and the spec itself never contains 2 consecutive
// spaces -- making the first such gap an unambiguous split point.
var flagLineRe = regexp.MustCompile(`(?m)^(\s*\S.*?)(\s{2,})(\S.*)$`)

func colorizeFlagNames(usages string) string {
	usages = strings.TrimRight(usages, "\n")
	return flagLineRe.ReplaceAllStringFunc(usages, func(line string) string {
		m := flagLineRe.FindStringSubmatch(line)
		return styScenario.Render(m[1]) + m[2] + m[3]
	})
}

func rpad(s string, padding int) string {
	return fmt.Sprintf(fmt.Sprintf("%%-%ds", padding), s)
}
