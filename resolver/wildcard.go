package resolver

import (
	"strings"
)

// HasWildcard checks if a path contains wildcard characters (* or ?)
func HasWildcard(path string) bool {
	return strings.ContainsAny(path, "*?")
}

// SplitWildcardPath splits a path into base path and wildcard pattern
// Example: "am/logs/*.log" -> ("am/logs/", "*.log")
func SplitWildcardPath(path string) (basePath, pattern string) {
	// Find the last / before any wildcard
	wildcardPos := strings.IndexAny(path, "*?")
	if wildcardPos == -1 {
		return path, ""
	}

	lastSlash := strings.LastIndex(path[:wildcardPos], "/")
	if lastSlash == -1 {
		return "", path
	}

	return path[:lastSlash+1], path[lastSlash+1:]
}

// MatchPattern checks if a name matches a wildcard pattern
// Supports * (any characters) and ? (single character)
func MatchPattern(name, pattern string) bool {
	return wildcardMatch(name, pattern)
}

// wildcardMatch implements simple wildcard matching
// * matches any sequence of characters
// ? matches any single character
func wildcardMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	if pattern == "*" {
		return true
	}

	// Simple implementation for common cases
	if !strings.Contains(pattern, "*") && !strings.Contains(pattern, "?") {
		return text == pattern
	}

	// Handle * wildcard
	if strings.Contains(pattern, "*") {
		parts := strings.Split(pattern, "*")
		if len(parts) == 2 {
			// Pattern like "*.log" or "test*"
			prefix := parts[0]
			suffix := parts[1]

			if prefix != "" && !strings.HasPrefix(text, prefix) {
				return false
			}
			if suffix != "" && !strings.HasSuffix(text, suffix) {
				return false
			}
			return true
		}
	}

	// For more complex patterns, use a simple character-by-character match
	return complexWildcardMatch(text, pattern)
}

// complexWildcardMatch handles more complex wildcard patterns
func complexWildcardMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	if pattern == "*" {
		return true
	}

	i, j := 0, 0
	starIdx, matchIdx := -1, 0

	for i < len(text) {
		if j < len(pattern) && (pattern[j] == '?' || pattern[j] == text[i]) {
			i++
			j++
		} else if j < len(pattern) && pattern[j] == '*' {
			starIdx = j
			matchIdx = i
			j++
		} else if starIdx != -1 {
			j = starIdx + 1
			matchIdx++
			i = matchIdx
		} else {
			return false
		}
	}

	for j < len(pattern) && pattern[j] == '*' {
		j++
	}

	return j == len(pattern)
}
