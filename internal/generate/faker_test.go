package generate

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// Every provider named in README.md's "Common providers" list plus every one
// used by scenarios/*.yaml has to resolve, because those scenarios ship with
// khaos and must keep working.
func TestFakerProvidersUsedByShippedScenarios(t *testing.T) {
	required := []string{
		// README.md "Common providers".
		"name", "email", "phone_number", "address", "street_address", "city",
		"country", "country_code", "postcode", "company", "job", "text", "word",
		"sentence", "url", "ipv4", "user_agent", "credit_card_number", "date",
		"date_time",
		// Used by scenarios/*.yaml.
		"date_this_month", "bothify",
	}

	for _, provider := range required {
		t.Run(provider, func(t *testing.T) {
			gen, err := NewFieldGen(scenario.Field{
				Name: "f", Type: scenario.FieldFaker, Provider: provider,
			}, testRand())
			if err != nil {
				t.Fatalf("provider %q must be supported: %v", provider, err)
			}
			for range 20 {
				v := gen()
				s, ok := v.(string)
				if !ok {
					// latitude/longitude are the only non-string providers, and
					// neither is in this list.
					t.Fatalf("provider %q produced %T, want a string", provider, v)
				}
				if s == "" {
					t.Fatalf("provider %q produced an empty value", provider)
				}
			}
		})
	}
}

// Provider output must match the expected shape for each format.
func TestFakerProviderShapes(t *testing.T) {
	tests := []struct {
		provider string
		match    *regexp.Regexp
	}{
		{provider: "email", match: regexp.MustCompile(`^[^@]+@[^@]+\.[^@]+$`)},
		{provider: "ipv4", match: regexp.MustCompile(`^\d{1,3}(\.\d{1,3}){3}$`)},
		{provider: "url", match: regexp.MustCompile(`^https?://`)},
		// Every date/datetime a provider returns is ISO-8601 formatted.
		{provider: "date", match: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)},
		{provider: "date_this_month", match: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)},
		{provider: "date_this_year", match: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)},
		{provider: "date_time", match: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)},
		{provider: "date_time_this_year", match: regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)},
		// Faker's bothify() is always called with no arguments, so it uses the
		// default "## ??" template: two digits, a space, two letters.
		{provider: "bothify", match: regexp.MustCompile(`^\d\d [a-zA-Z][a-zA-Z]$`)},
	}

	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			gen, err := NewFieldGen(scenario.Field{
				Name: "f", Type: scenario.FieldFaker, Provider: tt.provider,
			}, testRand())
			if err != nil {
				t.Fatalf("NewFieldGen: %v", err)
			}
			for range 50 {
				v := gen().(string)
				if !tt.match.MatchString(v) {
					t.Fatalf("provider %q produced %q, want it to match %s", tt.provider, v, tt.match)
				}
			}
		})
	}
}

// An unmapped provider has to fail loudly and say which one.
func TestUnknownFakerProviderIsActionable(t *testing.T) {
	_, err := NewFieldGen(scenario.Field{
		Name: "f", Type: scenario.FieldFaker, Provider: "nonexistent_provider_xyz",
	}, testRand())
	if err == nil {
		t.Fatal("want error, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"nonexistent_provider_xyz", `"f"`, "supported providers", "email"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q should mention %q", msg, want)
		}
	}
}

// gofakeit has no locale concept, so locale is accepted and ignored rather
// than rejected -- rejecting it would break scenarios that only want the
// shape of the data.
func TestFakerLocaleIsAcceptedAndIgnored(t *testing.T) {
	gen, err := NewFieldGen(scenario.Field{
		Name: "f", Type: scenario.FieldFaker, Provider: "name", Locale: "de_DE",
	}, testRand())
	if err != nil {
		t.Fatalf("locale must not be an error: %v", err)
	}
	withLocale := gen()

	plain, err := NewFieldGen(scenario.Field{
		Name: "f", Type: scenario.FieldFaker, Provider: "name",
	}, testRand())
	if err != nil {
		t.Fatalf("NewFieldGen: %v", err)
	}
	if withLocale != plain() {
		t.Error("locale changed the output; it is documented as a no-op")
	}
}

func TestFakerProvidersListIsSorted(t *testing.T) {
	list := FakerProviders()
	if len(list) != len(fakerProviders) {
		t.Fatalf("FakerProviders returned %d entries, want %d", len(list), len(fakerProviders))
	}
	for i := 1; i < len(list); i++ {
		if list[i-1] >= list[i] {
			t.Fatalf("FakerProviders is not sorted at %d: %q then %q", i, list[i-1], list[i])
		}
	}
}
