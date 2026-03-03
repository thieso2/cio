package resolver

import (
	"strings"
)

// HasWildcard checks if a path contains wildcard characters (* or ?)
func HasWildcard(path string) bool {
	return strings.ContainsAny(path, "*?")
}

// HasDoubleStarWildcard checks if a path contains a ** wildcard (recursive glob).
func HasDoubleStarWildcard(path string) bool {
	return strings.Contains(path, "**")
}

// MatchDoubleStarPattern matches a path against a glob pattern where:
//   - ** matches any sequence of characters including /
//   - *  matches any sequence of characters except /
//   - ?  matches any single character except /
func MatchDoubleStarPattern(path, pattern string) bool {
	return doubleStarMatch(path, pattern)
}

// doubleStarMatch is the recursive implementation for MatchDoubleStarPattern.
func doubleStarMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	// ** wildcard
	if len(pattern) >= 2 && pattern[0] == '*' && pattern[1] == '*' {
		// Skip all consecutive stars so *** is treated the same as **
		p := 2
		for p < len(pattern) && pattern[p] == '*' {
			p++
		}
		// **/ means zero or more complete path segments (each ending with /).
		// Try matching zero segments first (skip the **/ and match rest directly),
		// then consume one segment at a time and re-apply **/ on the remainder.
		if p < len(pattern) && pattern[p] == '/' {
			rest := pattern[p+1:] // pattern after **/
			if doubleStarMatch(text, rest) {
				return true
			}
			for i := 0; i < len(text); i++ {
				if text[i] == '/' {
					// Re-apply the full **/rest against the text after this /
					if doubleStarMatch(text[i+1:], pattern) {
						return true
					}
				}
			}
			return false
		}
		// ** not followed by /: matches zero or more characters including /
		suffix := pattern[p:]
		for i := 0; i <= len(text); i++ {
			if doubleStarMatch(text[i:], suffix) {
				return true
			}
		}
		return false
	}
	// Single * wildcard: matches any sequence of characters except /
	if pattern[0] == '*' {
		suffix := pattern[1:]
		for i := 0; i <= len(text); i++ {
			if i > 0 && text[i-1] == '/' {
				return false
			}
			if doubleStarMatch(text[i:], suffix) {
				return true
			}
		}
		return false
	}
	// ? wildcard: matches any single character except /
	if pattern[0] == '?' {
		if len(text) == 0 || text[0] == '/' {
			return false
		}
		return doubleStarMatch(text[1:], pattern[1:])
	}
	// Literal character
	if len(text) == 0 || text[0] != pattern[0] {
		return false
	}
	return doubleStarMatch(text[1:], pattern[1:])
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
