// Package limit expresses how fast a user may go.
//
// A [Limit] is a rate in requests per second, and a [Quota] pairs one with a
// burst ceiling. Build a Limit with [PerSecond], [PerMinute], [PerHour] or
// [Every] rather than converting a number, so the unit is visible where it is
// written: PerMinute(60) and PerSecond(1) are the same value, and only one of
// them says which the author meant.
package limit
