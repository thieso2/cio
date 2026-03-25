package certmanager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thieso2/cio/apilog"
	cm "google.golang.org/api/certificatemanager/v1"
)

func extractName(fullName string) string {
	if idx := strings.LastIndex(fullName, "/"); idx >= 0 {
		return fullName[idx+1:]
	}
	return fullName
}

func parseTime(s string) time.Time {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

// CertificateInfo holds information about a certificate.
type CertificateInfo struct {
	Name       string    `json:"name"`
	Domains    []string  `json:"domains,omitempty"`
	ExpireTime time.Time `json:"expire_time"`
	State      string    `json:"state"`
	Type       string    `json:"type"`
	Created    time.Time `json:"created"`
}

func (c *CertificateInfo) FormatShort() string { return c.Name }

func (c *CertificateInfo) FormatLong() string {
	expire := "-"
	if !c.ExpireTime.IsZero() {
		expire = c.ExpireTime.Format("2006-01-02")
	}
	domains := strings.Join(c.Domains, ", ")
	if len(domains) > 60 {
		domains = domains[:57] + "..."
	}
	return fmt.Sprintf("%-40s %-12s %-8s %-12s %s", c.Name, c.State, c.Type, expire, domains)
}

func CertificateLongHeader() string {
	return fmt.Sprintf("%-40s %-12s %-8s %-12s %s", "NAME", "STATE", "TYPE", "EXPIRES", "DOMAINS")
}

// CertMapInfo holds information about a certificate map.
type CertMapInfo struct {
	Name    string    `json:"name"`
	Targets int       `json:"targets"`
	Created time.Time `json:"created"`
}

func (m *CertMapInfo) FormatShort() string { return m.Name }

func (m *CertMapInfo) FormatLong() string {
	created := m.Created.Format("2006-01-02 15:04:05")
	return fmt.Sprintf("%-55s %7d  %s", m.Name, m.Targets, created)
}

func CertMapLongHeader() string {
	return fmt.Sprintf("%-55s %7s  %s", "NAME", "TARGETS", "CREATED")
}

// CertMapEntryInfo holds information about a certificate map entry.
type CertMapEntryInfo struct {
	Name         string   `json:"name"`
	Hostname     string   `json:"hostname,omitempty"`
	State        string   `json:"state"`
	Certificates []string `json:"certificates,omitempty"`
}

func (e *CertMapEntryInfo) FormatShort() string { return e.Name }

func (e *CertMapEntryInfo) FormatLong() string {
	certs := strings.Join(e.Certificates, ", ")
	hostname := e.Hostname
	if hostname == "" {
		hostname = "(primary)"
	}
	return fmt.Sprintf("%-40s %-40s %-12s %s", e.Name, hostname, e.State, certs)
}

func CertMapEntryLongHeader() string {
	return fmt.Sprintf("%-40s %-40s %-12s %s", "NAME", "HOSTNAME", "STATE", "CERTIFICATES")
}

// ListCertificates lists all certificates in a project.
func ListCertificates(ctx context.Context, project string) ([]*CertificateInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate manager service: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/global", project)
	apilog.Logf("[CertManager] Certificates.List(%s)", parent)

	var result []*CertificateInfo
	err = svc.Projects.Locations.Certificates.List(parent).Pages(ctx, func(resp *cm.ListCertificatesResponse) error {
		for _, cert := range resp.Certificates {
			info := &CertificateInfo{
				Name:    extractName(cert.Name),
				Domains: cert.SanDnsnames,
				Created: parseTime(cert.CreateTime),
				Type:    "managed",
			}
			if cert.ExpireTime != "" {
				info.ExpireTime = parseTime(cert.ExpireTime)
			}
			if cert.Managed != nil {
				info.State = cert.Managed.State
			}
			if cert.SelfManaged != nil {
				info.Type = "self"
			}
			result = append(result, info)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list certificates: %w", err)
	}
	return result, nil
}

// ListCertificateMaps lists all certificate maps in a project.
func ListCertificateMaps(ctx context.Context, project string) ([]*CertMapInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate manager service: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/global", project)
	apilog.Logf("[CertManager] CertificateMaps.List(%s)", parent)

	var result []*CertMapInfo
	err = svc.Projects.Locations.CertificateMaps.List(parent).Pages(ctx, func(resp *cm.ListCertificateMapsResponse) error {
		for _, m := range resp.CertificateMaps {
			result = append(result, &CertMapInfo{
				Name:    extractName(m.Name),
				Targets: len(m.GclbTargets),
				Created: parseTime(m.CreateTime),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list certificate maps: %w", err)
	}
	return result, nil
}

// ListCertificateMapEntries lists entries in a certificate map.
func ListCertificateMapEntries(ctx context.Context, project, mapName string) ([]*CertMapEntryInfo, error) {
	svc, err := GetService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate manager service: %w", err)
	}

	parent := fmt.Sprintf("projects/%s/locations/global/certificateMaps/%s", project, mapName)
	apilog.Logf("[CertManager] CertificateMapEntries.List(%s)", parent)

	var result []*CertMapEntryInfo
	err = svc.Projects.Locations.CertificateMaps.CertificateMapEntries.List(parent).Pages(ctx, func(resp *cm.ListCertificateMapEntriesResponse) error {
		for _, e := range resp.CertificateMapEntries {
			var certs []string
			for _, c := range e.Certificates {
				certs = append(certs, extractName(c))
			}
			result = append(result, &CertMapEntryInfo{
				Name:         extractName(e.Name),
				Hostname:     e.Hostname,
				State:        e.State,
				Certificates: certs,
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list certificate map entries: %w", err)
	}
	return result, nil
}
