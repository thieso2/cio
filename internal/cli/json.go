package cli

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/thieso2/cio/resource"
)

// printResourcesJSON outputs resources as a JSON array.
// Each element is the Metadata object if present, otherwise the ResourceInfo itself.
func printResourcesJSON(resources []*resource.ResourceInfo) error {
	items := make([]interface{}, 0, len(resources))
	for _, info := range resources {
		if info.Metadata != nil {
			items = append(items, info.Metadata)
		} else {
			items = append(items, info)
		}
	}
	return encodeJSON(items)
}

// printSingleJSON outputs a single object as JSON.
func printSingleJSON(v interface{}) error {
	return encodeJSON(v)
}

func encodeJSON(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}
	return nil
}
