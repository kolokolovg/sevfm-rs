---
name: changelog
description: Generate or update CHANGELOG.md entries from git history, PRs, and issues using Keep a Changelog format. Triggered by requests to update changelogs, document changes, or summarize what changed between versions.
---

# Changelog Generator

Generate CHANGELOG.md entries by analyzing code changes, PRs, and issues — not just commit messages.

## Gathering Context

When asked to generate a changelog entry (e.g., for a version or tag range), collect context from multiple sources. Commit messages are often vague or useless, so prioritize richer signals.

### Step 1: Determine the version range

```bash
# Get the two most recent tags
CURRENT_TAG=$(git describe --tags --abbrev=0)
PREVIOUS_TAG=$(git describe --tags --abbrev=0 "$CURRENT_TAG^")

# Or if the user specifies a version:
# CURRENT_TAG="v2.4.0"
# PREVIOUS_TAG=$(git describe --tags --abbrev=0 "$CURRENT_TAG^")

# Get the date of the previous tag for filtering PRs/issues
SINCE_DATE=$(git log -1 --format=%aI "$PREVIOUS_TAG")
```

### Step 2: Gather signals (in order of usefulness)

**PRs merged since last release:**
```bash
gh pr list --state merged --search "merged:>=$SINCE_DATE" --json number,title,body,labels --limit 100
```

**Issues closed since last release:**
```bash
gh issue list --state closed --search "closed:>=$SINCE_DATE" --json number,title,body,labels --limit 100
```

**Diff between tags:**
```bash
git diff "$PREVIOUS_TAG".."$CURRENT_TAG" -- '*.ts' '*.js' '*.tsx' '*.jsx' '*.py' '*.go' '*.rs' ':!*.lock' ':!*.min.*'
```

**Files changed:**
```bash
git diff --name-only "$PREVIOUS_TAG".."$CURRENT_TAG"
```

**New or changed test descriptions (reveal intent):**
```bash
git diff "$PREVIOUS_TAG".."$CURRENT_TAG" -- '**/*.test.*' '**/*.spec.*' '**/test_*' '**/*_test.*'
```

**Config and dependency changes:**
```bash
git diff "$PREVIOUS_TAG".."$CURRENT_TAG" -- 'package.json' '*.toml' '*.yaml' '*.yml' '*.env.example' '*.config.*'
```

**Commit messages (last resort):**
```bash
git log --oneline "$PREVIOUS_TAG".."$CURRENT_TAG"
```

## Categorizing Changes

Classify every change into one of these Keep a Changelog categories:

- **Added** — new features or capabilities
- **Changed** — changes to existing functionality
- **Deprecated** — features that will be removed
- **Removed** — features that were removed
- **Fixed** — bug fixes
- **Security** — vulnerability fixes

### Filtering rules

- **Skip** refactors, test-only changes, CI config, and code style changes unless they have user-facing impact
- **Group** minor internal improvements under a single "Internal improvements" bullet if worth mentioning at all
- **Prioritize** user-facing changes over developer-facing ones
- If you can't determine the user impact of a change, omit it or group it under internal improvements

## Writing Rules

- Lead with user benefit: "Add bulk CSV export for reports" not "Implement exportToCsv utility function"
- Never mention file names, function names, or technical internals in the output
- Use active voice: "Add dark mode" not "Dark mode has been added"
- Be specific: "Fix crash when opening files larger than 2GB" not "Fix file handling bug"
- Each entry should be one line. No paragraphs.

## Output Format

Write entries in [Keep a Changelog](https://keepachangelog.com/) format:

```markdown
## [2.4.0] - 2025-01-15

### Added
- Add bulk CSV export for all report types
- Add keyboard shortcuts for common actions (Ctrl+K to search, Ctrl+N for new)

### Changed
- Improve search performance for large datasets

### Fixed
- Fix crash when opening files larger than 2GB
- Fix incorrect totals on the billing summary page
```

## Example: Bad Commits → Good Changelog

**Typical commit messages:**
```
fix stuff
wip
update deps
refactor handler
PR #142: misc changes
```

**What the skill produces by analyzing PRs, issues, diffs, and test descriptions:**
```markdown
## [1.8.0] - 2025-01-10

### Added
- Add real-time collaboration for shared documents
- Add Slack notifications for failed deployments

### Fixed
- Fix duplicate entries appearing in search results
- Fix password reset emails not sending for SSO users
```

## Final Steps

- If a CHANGELOG.md already exists, prepend the new entry below the header
- If no CHANGELOG.md exists, create one with a header and the new entry
- Show the user what was generated and ask if they want to adjust anything before writing
