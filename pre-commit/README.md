# Pre-commit Hook Tool to Prevent Committing Sensitive Information

This document guides you how to use pre-commit hook and git-secrets to prevent committing sensitive information to a git repository. Simply running `git commit` will trigger the verification of the git-added files.

## Prerequisite

The following dependencies or tools are required.

- [pre-commit](https://pre-commit.com/#config-id)
- [git-secrets](https://github.com/awslabs/git-secrets)

For macos distributions, simply run `brew install`.

```bash
brew install pre-commit git-secrets
```

## Installation

run in the current repo:

```bash
pre-commit install
```

## Configuration

### Define Prohibited Patterns

Adds prohibited patterns to the current repo (the added patterns are stored in .git/config):

```bash
# pattern of the ak value
git secrets --add 'LTAI[A-Za-z0-9]+'

git secrets --add '[aA][cC][cC][eE][sS][sS].?[iI][dD]\s*=\s*.+'
git secrets --add '[aA][cC][cC][eE][sS][sS].?[kK][eE][yY]\s*=\s*.+'
git secrets --add '[aA][cC][cC][eE][sS][sS].?[sS][eE][cC][rR][eE][tT]\s*=\s*.+'
```

### Ignoring False Positives

Sometimes a regular expression might match false positives. For example, write one line code to setup access key from a outer confifuration file look a lot like the pattern of `[aA][cC][cC][eE][sS][sS]*[kK][eE][yY]\s*=\s*.+`. You can specify many different regular expression patterns as false positives using the following command:

```bash
git secrets --add --allowed --literal 'code line'
```

or skip current one-time false positive

```bash
git commit --no-verify ...
```
