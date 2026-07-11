# Git Hooks

This directory contains git hooks that are version-controlled with the project.

## Setup

After cloning the repository, run:

```bash
git config core.hooksPath .githooks
```

This configures git to use the hooks in this directory instead of `.git/hooks/`.

## Available Hooks

- `pre-commit`: Runs before each commit to ensure code quality
  - Checks formatting with `cargo fmt --check`
  - Runs clippy with `cargo clippy`
  - Runs tests with `cargo test`

If any check fails, the commit will be blocked.
