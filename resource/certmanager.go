package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/thieso2/cio/certmanager"
	"github.com/thieso2/cio/resolver"
)

const TypeCertManager Type = "certs"

type CertManagerResource struct {
	formatter PathFormatter
	subType   string // "", "maps", "maps/name/entries"
}

func CreateCertManagerResource(formatter PathFormatter) *CertManagerResource {
	return &CertManagerResource{formatter: formatter}
}

func (r *CertManagerResource) Type() Type        { return TypeCertManager }
func (r *CertManagerResource) SupportsInfo() bool { return false }

// parseCertsPath parses certs://[sub/path]
// certs://           → subType="", name=""
// certs://maps       → subType="maps", name=""
// certs://maps/name  → subType="maps", name="name"
// certs://maps/name/entries → subType="entries", mapName="name"
// certs://pattern*   → subType="", name="pattern*" (wildcard certs)
func parseCertsPath(path string) (subType, name, mapName string) {
	rest := strings.TrimPrefix(path, "certs://")
	if rest == "" {
		return "", "", ""
	}
	parts := strings.Split(rest, "/")
	if parts[0] == "maps" {
		if len(parts) == 1 {
			return "maps", "", ""
		}
		if len(parts) >= 3 && parts[2] == "entries" {
			return "entries", "", parts[1]
		}
		return "maps", parts[1], ""
	}
	// Otherwise it's a certificate name/pattern
	return "", rest, ""
}

func (r *CertManagerResource) List(ctx context.Context, path string, opts *ListOptions) ([]*ResourceInfo, error) {
	var project string
	if opts != nil {
		project = opts.ProjectID
	}
	if project == "" {
		return nil, fmt.Errorf("project ID is required for certificate manager resources")
	}

	subType, name, mapName := parseCertsPath(path)
	r.subType = subType

	switch subType {
	case "":
		return r.listCertificates(ctx, project, name)
	case "maps":
		return r.listCertMaps(ctx, project, name)
	case "entries":
		return r.listCertMapEntries(ctx, project, mapName)
	default:
		return nil, fmt.Errorf("unknown certs sub-type: %s", subType)
	}
}

func (r *CertManagerResource) listCertificates(ctx context.Context, project, pattern string) ([]*ResourceInfo, error) {
	certs, err := certmanager.ListCertificates(ctx, project)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, cert := range certs {
		if pattern != "" && !resolver.MatchPattern(cert.Name, pattern) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     cert.Name,
			Path:     "certs://" + cert.Name,
			Type:     "certificate",
			Created:  cert.Created,
			Modified: cert.Created,
			Metadata: cert,
		})
	}
	return resources, nil
}

func (r *CertManagerResource) listCertMaps(ctx context.Context, project, pattern string) ([]*ResourceInfo, error) {
	maps, err := certmanager.ListCertificateMaps(ctx, project)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, m := range maps {
		if pattern != "" && !resolver.MatchPattern(m.Name, pattern) {
			continue
		}
		resources = append(resources, &ResourceInfo{
			Name:     m.Name,
			Path:     "certs://maps/" + m.Name,
			Type:     "cert-map",
			Created:  m.Created,
			Modified: m.Created,
			Metadata: m,
		})
	}
	return resources, nil
}

func (r *CertManagerResource) listCertMapEntries(ctx context.Context, project, mapName string) ([]*ResourceInfo, error) {
	entries, err := certmanager.ListCertificateMapEntries(ctx, project, mapName)
	if err != nil {
		return nil, err
	}
	var resources []*ResourceInfo
	for _, e := range entries {
		resources = append(resources, &ResourceInfo{
			Name:     e.Name,
			Path:     fmt.Sprintf("certs://maps/%s/entries/%s", mapName, e.Name),
			Type:     "cert-map-entry",
			Metadata: e,
		})
	}
	return resources, nil
}

func (r *CertManagerResource) Remove(ctx context.Context, path string, opts *RemoveOptions) error {
	return fmt.Errorf("removing certificate resources is not supported via cio")
}

func (r *CertManagerResource) Info(ctx context.Context, path string) (*ResourceInfo, error) {
	return nil, fmt.Errorf("use 'cio ls -l certs://' for certificate details")
}

func (r *CertManagerResource) ParsePath(path string) (*PathComponents, error) {
	return &PathComponents{ResourceType: TypeCertManager}, nil
}

func (r *CertManagerResource) FormatShort(info *ResourceInfo, _ string) string {
	switch v := info.Metadata.(type) {
	case *certmanager.CertificateInfo:
		return v.FormatShort()
	case *certmanager.CertMapInfo:
		return v.FormatShort()
	case *certmanager.CertMapEntryInfo:
		return v.FormatShort()
	}
	return info.Name
}

func (r *CertManagerResource) FormatLong(info *ResourceInfo, _ string) string {
	switch v := info.Metadata.(type) {
	case *certmanager.CertificateInfo:
		return v.FormatLong()
	case *certmanager.CertMapInfo:
		return v.FormatLong()
	case *certmanager.CertMapEntryInfo:
		return v.FormatLong()
	}
	return info.Name
}

func (r *CertManagerResource) FormatDetailed(info *ResourceInfo, aliasPath string) string {
	return r.FormatLong(info, aliasPath)
}

func (r *CertManagerResource) FormatHeader() string {
	return r.FormatLongHeader()
}

func (r *CertManagerResource) FormatLongHeader() string {
	switch r.subType {
	case "maps":
		return certmanager.CertMapLongHeader()
	case "entries":
		return certmanager.CertMapEntryLongHeader()
	default:
		return certmanager.CertificateLongHeader()
	}
}
