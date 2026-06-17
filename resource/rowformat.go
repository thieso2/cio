package resource

// RowFormatter is implemented by the per-service *Info metadata structs that know
// how to render themselves as a short or long listing row. Resources whose List()
// stores such a value in ResourceInfo.Metadata delegate formatting here instead of
// type-switching the concrete type back out of interface{} — so adding a new
// metadata type needs no change to the resource's FormatShort/FormatLong.
type RowFormatter interface {
	FormatShort() string
	FormatLong() string
}

// metaShort renders info.Metadata as a short row, falling back to the name when
// the metadata does not know how to format itself.
func metaShort(info *ResourceInfo) string {
	if rf, ok := info.Metadata.(RowFormatter); ok {
		return rf.FormatShort()
	}
	return info.Name
}

// metaLong renders info.Metadata as a long row, falling back to the name.
func metaLong(info *ResourceInfo) string {
	if rf, ok := info.Metadata.(RowFormatter); ok {
		return rf.FormatLong()
	}
	return info.Name
}
