package generate

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"

	"github.com/aleksandarskrbic/khaos/internal/scenario"
)

// isoDate and isoDateTime are the ISO-8601 layouts applied to every
// date/datetime a faker provider returns.
const (
	isoDate     = "2006-01-02"
	isoDateTime = "2006-01-02T15:04:05Z07:00"
)

// fakerProviders maps a scenario `provider:` name to a gofakeit call.
//
// Every provider used by scenarios/*.yaml and every provider documented in
// README.md's "Common providers" list is mapped; anything else is a
// construction error naming the provider, with no silent fallback to filler
// text -- a scenario that asks for `provider: iban` fails loudly at load
// time.
var fakerProviders = map[string]func(*gofakeit.Faker) any{
	// People and contact.
	"name":         func(f *gofakeit.Faker) any { return f.Name() },
	"first_name":   func(f *gofakeit.Faker) any { return f.FirstName() },
	"last_name":    func(f *gofakeit.Faker) any { return f.LastName() },
	"email":        func(f *gofakeit.Faker) any { return f.Email() },
	"phone_number": func(f *gofakeit.Faker) any { return f.PhoneFormatted() },
	"ssn":          func(f *gofakeit.Faker) any { return f.SSN() },

	// Address. gofakeit's AddressInfo.Address renders a full postal address on
	// one line.
	"address":        func(f *gofakeit.Faker) any { return f.Address().Address },
	"street_address": func(f *gofakeit.Faker) any { return f.Street() },
	"city":           func(f *gofakeit.Faker) any { return f.City() },
	"state":          func(f *gofakeit.Faker) any { return f.State() },
	"country":        func(f *gofakeit.Faker) any { return f.Country() },
	"country_code":   func(f *gofakeit.Faker) any { return f.CountryAbr() },
	"postcode":       func(f *gofakeit.Faker) any { return f.Zip() },
	"zipcode":        func(f *gofakeit.Faker) any { return f.Zip() },
	"latitude":       func(f *gofakeit.Faker) any { return f.Latitude() },
	"longitude":      func(f *gofakeit.Faker) any { return f.Longitude() },

	// Company and work.
	"company": func(f *gofakeit.Faker) any { return f.Company() },
	"job":     func(f *gofakeit.Faker) any { return f.JobTitle() },

	// Text.
	"text":      func(f *gofakeit.Faker) any { return f.Paragraph() },
	"word":      func(f *gofakeit.Faker) any { return f.Word() },
	"sentence":  func(f *gofakeit.Faker) any { return f.Sentence() },
	"paragraph": func(f *gofakeit.Faker) any { return f.Paragraph() },

	// Internet.
	"url": func(f *gofakeit.Faker) any { return f.URL() },
	"domain_name": func(f *gofakeit.Faker) any {
		return f.DomainName()
	},
	"ipv4": func(f *gofakeit.Faker) any { return f.IPv4Address() },
	"ipv6": func(f *gofakeit.Faker) any { return f.IPv6Address() },
	"mac_address": func(f *gofakeit.Faker) any {
		return f.MacAddress()
	},
	"user_agent": func(f *gofakeit.Faker) any { return f.UserAgent() },
	"uuid4":      func(f *gofakeit.Faker) any { return f.UUID() },

	// Finance.
	"credit_card_number": func(f *gofakeit.Faker) any { return f.CreditCardNumber(nil) },
	"currency_code":      func(f *gofakeit.Faker) any { return f.CurrencyShort() },

	// Dates. Every provider here emits an ISO-8601 string, never a time.Time.
	"date":      func(f *gofakeit.Faker) any { return f.Date().UTC().Format(isoDate) },
	"date_time": func(f *gofakeit.Faker) any { return f.Date().UTC().Format(isoDateTime) },
	"date_this_month": func(f *gofakeit.Faker) any {
		start, end := monthToDate(time.Now())
		return f.DateRange(start, end).UTC().Format(isoDate)
	},
	"date_this_year": func(f *gofakeit.Faker) any {
		start, end := yearToDate(time.Now())
		return f.DateRange(start, end).UTC().Format(isoDate)
	},
	"date_time_this_month": func(f *gofakeit.Faker) any {
		start, end := monthToDate(time.Now())
		return f.DateRange(start, end).UTC().Format(isoDateTime)
	},
	"date_time_this_year": func(f *gofakeit.Faker) any {
		start, end := yearToDate(time.Now())
		return f.DateRange(start, end).UTC().Format(isoDateTime)
	},

	// Patterned filler. bothify always uses the default "## ??" template: two
	// digits, a space, two letters. A `pattern:` key next to `provider:
	// bothify` in a scenario is not part of the field schema and is ignored.
	"bothify": func(f *gofakeit.Faker) any { return generatePattern(f, "## ??") },
	"numerify": func(f *gofakeit.Faker) any {
		return generatePattern(f, "###")
	},
	"lexify": func(f *gofakeit.Faker) any {
		return generatePattern(f, "????")
	},
}

// newFakerGen builds the generator for a `type: faker` field.
//
// Locale is accepted and ignored: gofakeit has no locale concept, so
// `locale: de_DE` silently produces the same en_US-flavoured data as
// everything else. That is a documented no-op rather than an error, since
// rejecting it would break existing scenarios for no gain.
// o is unused: faker fields have no cardinality cache, so no fill loop to bound.
func newFakerGen(f scenario.Field, r *rand.Rand, _ options) (func() any, error) {
	if f.Provider == "" {
		return nil, fmt.Errorf("faker field %q requires 'provider'", f.Name)
	}
	fn, ok := fakerProviders[f.Provider]
	if !ok {
		return nil, fmt.Errorf(
			"faker field %q: unsupported faker provider %q; supported providers are: %s",
			f.Name, f.Provider, strings.Join(FakerProviders(), ", "))
	}
	// Driving gofakeit from the same *rand.Rand keeps a seeded run reproducible
	// end to end. lock=false because generators are single-goroutine by contract.
	fake := gofakeit.NewFaker(randSource{r}, false)
	return func() any { return fn(fake) }, nil
}

// FakerProviders lists the supported `provider:` names, sorted. Exported so the
// CLI can show them when a scenario names an unsupported provider.
func FakerProviders() []string {
	out := make([]string, 0, len(fakerProviders))
	for name := range fakerProviders {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// randSource adapts *rand.Rand to the math/rand/v2 Source gofakeit wants, so
// faker values come out of the same stream as every other field.
type randSource struct{ r *rand.Rand }

func (s randSource) Uint64() uint64 { return s.r.Uint64() }

// generatePattern expands a gofakeit template ('#' -> digit, '?' -> letter).
//
// gofakeit.Generate only errors on malformed templates, and every template here
// is a constant, so a failure would be a bug in this file rather than bad user
// input; the raw template is a harmless last resort.
func generatePattern(f *gofakeit.Faker, pattern string) string {
	s, err := f.Generate(pattern)
	if err != nil {
		return pattern
	}
	return s
}

// monthToDate returns the first instant of now's month and now itself: the
// window for the date_this_month provider.
func monthToDate(now time.Time) (time.Time, time.Time) {
	// Both ends are derived from the same UTC instant: mixing a local-calendar
	// start with a UTC end can invert the range in the first hours of a month.
	end := now.UTC()
	start := time.Date(end.Year(), end.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, end
}

// yearToDate returns January 1st of now's year and now itself: the window for
// the date_this_year provider.
func yearToDate(now time.Time) (time.Time, time.Time) {
	end := now.UTC()
	start := time.Date(end.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
	return start, end
}
