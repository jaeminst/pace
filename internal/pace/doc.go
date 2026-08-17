// Package pace implements github.com/jaeminst/pace.
//
// It carries the same package name as the facade that exports it, so that this
// move stayed invisible: %T still prints *pace.Limiter, panic messages and
// reflection are unchanged, and the test suite kept every `pace.` reference it
// already had.
//
// The user-facing documentation — what the package is for, how Limiter and
// Client relate, the error model, the compatibility promise — lives on the
// facade in the repository root, which is where a caller reads it. What lives
// here is the reference material the facade cannot render: the doc comment on
// every field, method and interface. Editors resolve the aliases and show
// these, so this is not a second copy of anything.
package pace
