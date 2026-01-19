# Release Process

This document describes how to create a new release of `cio`.

## Overview

The project uses [GoReleaser](https://goreleaser.com/) for automated builds and releases. When you push a version tag, GitHub Actions automatically:

1. Builds binaries for multiple platforms (Linux, macOS, Windows)
2. Creates a GitHub release with changelog
3. Uploads all binaries and checksums

## Supported Platforms

- **Linux**: amd64, arm64
- **macOS**: amd64 (Intel), arm64 (Apple Silicon)
- **Windows**: amd64

## Creating a Release

### 1. Ensure Everything is Ready

```bash
# Make sure you're on the main/master branch (or your target branch)
git checkout main
git pull origin main

# Run tests to ensure everything works
mise test

# Check that the build works
mise build
```

### 2. Create and Push a Version Tag

We follow [Semantic Versioning](https://semver.org/):
- `v1.0.0` - Major version (breaking changes)
- `v1.1.0` - Minor version (new features, backwards compatible)
- `v1.0.1` - Patch version (bug fixes)

```bash
# Create a new tag (replace with your version)
git tag -a v1.0.0 -m "Release v1.0.0"

# Push the tag to trigger the release workflow
git push origin v1.0.0
```

### 3. Monitor the Release

1. Go to the [Actions tab](https://github.com/thieso2/cio/actions) in GitHub
2. Watch the "Release" workflow
3. Once complete, check the [Releases page](https://github.com/thieso2/cio/releases)

### 4. Verify the Release

The release should include:
- Binaries for all supported platforms
- SHA256 checksums (`checksums.txt`)
- Automatically generated changelog
- Source code archives

## Local Testing with GoReleaser

You can test the release process locally without publishing:

```bash
# Install GoReleaser (if not already installed)
brew install goreleaser  # macOS
# or
go install github.com/goreleaser/goreleaser/v2@latest

# Test the release configuration (snapshot mode, no publishing)
goreleaser release --snapshot --clean

# Binaries will be in the dist/ directory
ls -lh dist/
```

## Version Information

The build process embeds version information into the binary using ldflags:

- **version**: The git tag (e.g., `v1.0.0`)
- **commit**: The git commit hash
- **date**: Build timestamp
- **builtBy**: Set to `goreleaser` for releases

You can check the version:

```bash
cio version
```

Example output:
```
cio version v1.0.0
  commit: abc123def
  built:  2024-01-19T10:30:00Z
  by:     goreleaser
```

## Manual Release (Advanced)

If you need to create a release manually:

```bash
# Set environment variables
export GITHUB_TOKEN="your-github-token"

# Run GoReleaser
goreleaser release --clean
```

## Troubleshooting

### Release Workflow Fails

1. Check the [Actions logs](https://github.com/thieso2/cio/actions)
2. Common issues:
   - Go version mismatch: Update `.github/workflows/release.yml`
   - GoReleaser config error: Validate with `goreleaser check`
   - Missing permissions: Check `GITHUB_TOKEN` permissions

### Tag Already Exists

If you need to re-create a tag:

```bash
# Delete local tag
git tag -d v1.0.0

# Delete remote tag
git push origin :refs/tags/v1.0.0

# Create and push new tag
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

## Changelog

The changelog is automatically generated from commit messages. Use conventional commits for better changelog organization:

- `feat:` - New features
- `fix:` - Bug fixes
- `refactor:` - Code refactoring
- `perf:` - Performance improvements
- `docs:` - Documentation changes
- `test:` - Test changes
- `chore:` - Maintenance tasks

Example:
```bash
git commit -m "feat: add BigQuery table wildcards support"
git commit -m "fix: handle empty dataset names correctly"
git commit -m "refactor: simplify metadata caching logic"
```

## Pre-release Versions

To create a pre-release (beta, alpha, rc):

```bash
# Create pre-release tag
git tag -a v1.0.0-beta.1 -m "Release v1.0.0-beta.1"
git push origin v1.0.0-beta.1
```

GoReleaser will automatically mark it as a pre-release on GitHub.

## Next Steps

After a successful release:

1. Announce the release (if applicable)
2. Update documentation if needed
3. Close related issues/milestones on GitHub
4. Update any dependent projects
