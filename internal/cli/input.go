package cli

import (
	"github.com/thieso2/cio/resolver"
	"github.com/thieso2/cio/resource"
)

// This file is the one home for the command prelude: turning a raw user argument
// (an alias like ":am/x" or a direct path like "gs://bucket/x") into a resolved
// full path and a ready resource handler. ls/rm/info used to each open with a
// copy of this dance, and the "is this already a direct path?" check had drifted
// into three different subsets of the scheme list. Now they all ask
// resolver.IsDirectPath — the single canonical union.

// resolveInput turns a user-supplied path or alias into a full path, reporting
// whether the input was an alias (so callers know whether to reverse-map output).
func resolveInput(path string) (r *resolver.Resolver, fullPath string, wasAlias bool, err error) {
	r = resolver.Create(cfg)
	if resolver.IsDirectPath(path) {
		return r, path, false, nil
	}
	fullPath, err = r.Resolve(path)
	if err != nil {
		return nil, "", false, err
	}
	return r, fullPath, true, nil
}

// newResourceFactory builds a factory whose output paths reverse-map to aliases
// only when the user actually supplied an alias (an identity formatter otherwise,
// so direct-path input echoes direct paths back).
func newResourceFactory(r *resolver.Resolver, wasAlias bool) *resource.Factory {
	formatter := resource.PathFormatter(func(p string) string { return p })
	if wasAlias {
		formatter = r.ReverseResolve
	}
	factory := resource.CreateFactory(formatter)
	factory.BillingTable = cfg.Billing.Table
	factory.Region = cfg.Defaults.Region
	return factory
}

// resolveToResource is the full prelude: resolve the input and build the resource
// handler for it. Commands that need the factory before the handler (e.g. info's
// BigQuery-wildcard branch) use resolveInput + newResourceFactory instead.
func resolveToResource(path string) (res resource.Resource, r *resolver.Resolver, fullPath string, wasAlias bool, err error) {
	r, fullPath, wasAlias, err = resolveInput(path)
	if err != nil {
		return nil, nil, "", false, err
	}
	res, err = newResourceFactory(r, wasAlias).Create(fullPath)
	if err != nil {
		return nil, nil, "", false, err
	}
	return res, r, fullPath, wasAlias, nil
}
