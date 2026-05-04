# Contributing to `tar-diff`

Thank you for your interest in `tar-diff`! We appreciate contributions that help improve this library and maintain high code quality. This guide covers everything you need to know to contribute effectively to the project.   

This project is licensed under the [Apache License, Version 2.0](LICENSE). By contributing, you agree that your contributions will be licensed under the same terms.

## Prerequisites

Before you begin, ensure you have the following installed:

- golang >= 1.26 (see [`go.mod`](go.mod))
- `make`
- `tar`
- `diffutils`, `bzip2`, `gzip` (for tests)

`tar-diff` is implemented in Go, so familiarity with the language is helpful.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/tar-diff.git
   cd tar-diff
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/containers/tar-diff.git
   ```
4. **Build the project**:
   ```bash
   make
   ```

## Development Workflow

1. **Create a new branch** for your work:
   ```bash
   git checkout -b my-feature-branch
   ```

2. **Make your changes**

3. **Run tests** to ensure everything works:
   ```bash
   make test
   ```

4. **Run validation** to check code quality:
   ```bash
   make validate
   ```
5. **Add and commit your changes** (see [Git Commit Style](#Git-Commit-Style) below):
   ```bash
   git add <file>
   git commit
   ```

6. **Push your branch** to your fork:
   ```bash
   git push origin my-feature-branch
   ```

7. **Open a pull request (PR)** against the `main` branch of the upstream repository

## Sign Your Commits

All commits must be signed off to certify that you agree to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).

To sign your commits, use the `-s` flag.

This adds a `Signed-off-by` line to your commit message:
```
Signed-off-by: Your Name <your.email@example.com>
```

If you forget to sign a commit, you can amend it:
```bash
git commit --amend -s
```

**Note**: All commits in your PR must be signed off. PRs with unsigned commits will not be merged.

## Commit Signature Verification

All commits must have **verified signatures** (shown as a "Verified" badge on GitHub). This is enforced by branch protection rules - commits without verified signatures will be rejected.

To set up commit signing, see GitHub's documentation on [commit signature verification](https://docs.github.com/en/authentication/managing-commit-signature-verification).

## Git Commit Style

This project follows the [Conventional Commits](https://www.conventionalcommits.org) specification.

### Commit Message Format

```
<type>: <description>

[optional body]

[optional footer(s)]

Signed-off-by: Your Name <your.email@example.com>
```

### Commit Types

- **feat**: A new feature
- **fix**: A bug fix
- **docs**: Documentation changes
- **test**: Adding or updating tests
- **ci**: Changes to CI/CD workflows
- **refactor**: Code refactoring without changing functionality
- **perf**: Performance improvements
- **chore**: Maintenance tasks, dependency updates


## Testing

Before submitting a PR, ensure all local tests pass:

**Run all tests**:
```bash
make test
```

**Run unit tests only**:
```bash
make unit-test
```

**Run integration tests only**:
```bash
make integration-test
```

### Check test coverage:
Test coverage reports are generated in `test/coverage/` after running tests.

## Code Quality

**Format your code**:
```bash
make fmt
```

**Run linting and validation**:
```bash
make validate
```

This runs:
- `golangci-lint` for linting
- `go vet` for static analysis
- `gofmt` for formatting

All validation checks must pass before your PR can be merged.

### Cross-Platform Compatibility

`tar-diff` supports Linux, macOS, and Windows. When making changes:

- Avoid platform-specific code unless absolutely necessary
- Test on multiple platforms when possible
- The CI pipeline will test your changes on all supported platforms

### Pull Request Process

1. **Ensure your PR**:
   - Has a clear description of what it does and why
   - References any related issues (see below)
   - Passes all CI checks
   - Has all commits signed off (DCO)
   - Follows the commit message conventions

2. **Link PRs to Issues**:
   If your PR addresses an existing issue, link it in the PR description using GitHub keywords:
   - `Fixes #123`, `Closes #123`, `Resolves #123` - Automatically closes the issue when the PR is merged
   - `Related to #123`, `Addresses #123` - Links the PR without auto-closing

3. **Code review**:
   - Maintainers will review your PR
   - Address any feedback or requested changes
   - Keep your branch up to date with `main` to minimize merge conflicts
   - **Comment resolution**: Either the reviewer or the PR author is responsible for resolving it once the feedback has been addressed
   - **Approvals required**: PRs require at least **2 approvals** from maintainers before they can be merged

4. **After approval**:
   - Once your PR has the required approvals and all comments are resolved, a maintainer will merge your PR

## Merging and Squashing

This project values **clean, readable git history**. Code is read more often than it's written, so maintaining meaningful history for tools like `git log`, `git blame`, and `git revert` is important.

- **Use merge commits** (default) when the PR contains clean, individual, well-structured commits that each serve a purpose
- **Use squash merge** only when the PR contains fixup commits, debug iterations, or review feedback that doesn't need to be preserved in history


## Questions or Issues?

If you have questions or run into issues:

- Check existing [issues](https://github.com/containers/tar-diff/issues)
- Open a new issue if needed
- Feel free to ask questions in your PR

Thank you for contributing to `tar-diff` :)!
