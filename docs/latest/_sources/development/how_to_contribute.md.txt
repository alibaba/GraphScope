# How to Contribute 

**Thanks for your interest in the GraphScope project.**

GraphScope is an open-source project focused on large-scale graph computation with a friendly community of developers eager to help new contributors.
We welcome contributions of all types, from code improvements to documentation.

## Code of Conduct

Before contributing to or engaging with our community, please read our [Code of Conduct](https://github.com/alibaba/GraphScope/blob/main/CODE_OF_CONDUCT.md).

## Community

A good first step to getting involved in the GraphScope project is to participate in our discussions and join us in our different communication channels.
Here are several ways to connect with us:

### GitHub Discussion

We use [GitHub Discussion](https://github.com/alibaba/GraphScope/discussions) to ask and answer questions. Feel free to ask any questions you may have about GraphScope or issues you've encountered with your own code.

### Slack

Join in the [Slack channel](http://slack.graphscope.io) for discussion.

## Reporting Bugs

If you find a bug in GraphScope, first test against the [latest version of GraphScope](https://github.com/alibaba/GraphScope/tree/main) 
to ensure your issue hasn't already been fixed. If not, search our [issues list](https://github.com/alibaba/GraphScope/issues)
on GitHub to see if a similar issue has already been opened.

If you confirm that the bug hasn't already been reported [file a bug issue](https://github.com/alibaba/GraphScope/issues/new?assignees=&labels=&template=bug_report.md&title=%5BBUG%5D+) 
before writing any code. When submitting an issue, please include a clear and concise description of the problem,
relevant code or error messages, and steps to reproduce the issue.

## Requesting Features

If you find yourself wishing for a feature that doesn't exist in GraphScope, please open [a feature request issue](https://github.com/alibaba/GraphScope/issues/new?assignees=&labels=&template=feature_request.md&title=)
on GitHub to describe the feature, why it's needed, and how it should work. Other users may share similar needs,
and many of GraphScope's features were added because users saw a need.

## Improving Documentation

A great way to contribute to the project is by improving documentation. If you find any incomplete or inaccurate documentation, 
please share your knowledge with the community.

Documentation improvements are also a great way to gain some experience with our submission and review process, discussed below, without requiring a lot of local development environment setup. 
In fact, many documentation-only changes can be made directly in the [GraphScope document pages](https://graphscope.io/docs/) by clicking the “Edit On Github” button. This will handle making a fork and a pull request for you.

TBF: add a link to how to helping with documentation
TBF: add a link to how to build the documentation


## Contributing Code and Documentation Changes

If you would like to contribute a new feature or a bug fix to GraphScope, please first discuss your idea on a GitHub issue.
If there isn't an issue for it, create one. There may be someone already working on it, or it may have particular complexities
that you should be aware of before starting to code. There are often several ways to fix a problem, so it's important to find
the right approach before spending time on a PR that can't be merged.

### Install pre-commit

GraphScope use `pre-commit`_ to ensure no secrets are accidentally committed
into the Git repository. Before contributing, install `pre-commit` by typing:

```bash
$ pip3 install pre-commit
```

Configure the necessary pre-commit hooks with:

```bash
$ pre-commit install  --install-hooks
```

### Minor Fixes

For minor changes that affect documentation, you don't need to open a GitHub issue. Instead,
add the prefix "[MINOR]" to the title of your PR if it meets the following guidelines:

*  Grammar, usage and spelling fixes that affect no more than 2 files
*  Documentation updates affecting no more than 2 files and not more
   than 500 words.

### Fork and Create a Branch

Fork the main GraphScope code and clone it to your local machine. See
[GitHub help page](https://help.github.com/articles/fork-a-repo) for help.

Create a branch with a descriptive name.

A good branch name would be (where issue #42 is the ticket you're working on):

```bash
$ git checkout -b 42-add-chinese-translations
```

### Get the Test Suite Running

See [our how-to guide on testing](./how_to_test.md) for help.

### Implement Your Fix or Feature

At this point, you're ready to make the changes. Feel free to ask for help because everyone is a beginner at first!

### Get the Code Format and Style Right

Your patch should follow the same conventions and pass the same code quality checks as the rest of the project.
Follow [our code style guide](./code_style_guide.md) to attain the proper code format and style.

### Submitting Your Changes

See [our guide on how to submit a pull request](./how_to_submit_pr.md).

### Discussing and Keeping Your Pull Request Updated

You will probably receive feedback or requests for changes to your pull request.
This is big part of the submission process and is necessary to evaluate your changes correctly,
so don't be discouraged!

If a maintainer asks you to "rebase" your pull request, it means that a lot of code has changed and
you need to update your branch so it's easier to merge. To learn more about rebasing in Git, refer to the recommended workflow:

```bash
$ git checkout 42-add-chinese-translations
$ git pull --rebase upstream main
$ git push --force-with-lease 42-add-chinese-translations
```

Feel free to comment in the pull request to ping reviewers if you're awaiting a response.
If you encounter unfamiliar words or acronyms, refer to this [glossary](https://chromium.googlesource.com/chromiumos/docs/+/HEAD/glossary.md).

### Merging a PR (maintainers only)

A pull request can only be merged into main by a maintainer if:

* It is passing CI.
* At least two maintainers have approved it. If a maintainer opened the PR, only one extra approval is needed.
* There are no requested changes.
* It is up to date with current main.

### Shipping a Release (maintainers only)

TODO(dongze): TBF


## How to Review Pull Requests

We welcome contributions from the community and encourage everyone to review pull requests. When reviewing a pull request, please consider the following:

- Does the code follow our [Code of Conduct][1]?
- Does the code solve the problem described in the issue or feature request?
- Are there any potential side effects or edge cases that need to be considered?
- Are there any tests included to ensure the code works as expected?

If you have any questions or concerns about a pull request, please comment on the pull request or reach out to the contributor directly.

## Continuous integration testing

All pull requests that contain changes to code must be run through
continuous integration (CI) testing at [Github Actions](https://github.com/alibaba/GarphScope/actions)

The pull request change will trigger a CI testing run. Ideally, the code change
will pass ("be green") on all platform configurations supported by GraphScope.
This means that all tests pass and there are no linting errors. In reality,
however, it is not uncommon for the CI infrastructure itself to fail on specific
platforms ("be red"). It is vital to visually inspect the results of all failed ("red") tests
to determine whether the failure was caused by the changes in the pull request.
