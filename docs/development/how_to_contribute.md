# How to Contribute 

**Thanks for your interest in the GraphScope project.**

GraphScope is a large project and may seem overwhelming when you’re first getting involved. Contributing code is great,
but that’s probably not the first place to start. There are lots of ways to make valuable contributions to the project and community, 
from improving the documentation, submitting bug reports and feature requests or writing code which can be incorporated into GraphAr itself.

## Code of Conduct

Please read our [Code of Conduct][1] before contributing or engaging in our community.

## Community

A good first step to getting involved in the GraphScope project is to join us and participate in discussions where you can. there are several ways to connect with us.

### GitHub Discussion

We use [GitHub Discussion](https://github.com/alibaba/GraphScope/discussions) as a way to ask questions. Feel free to ask qustion if you have questions about
GraphScope or issues with your own code.

### Slack

TBF

### DingTalk

TBF

## Bug Report

If you find a bug in GraphAr, first make sure that you are testing against the [latest version of GraphScope](https://github.com/alibaba/GraphScope/tree/main), 
your issue may already have been fixed. If not, search our [issues list](https://github.com/alibaba/GraphScope/issues)
on GitHub in case a similar issue has already been opened.

If you get confirmation of your bug, [file a bug issue](https://github.com/alibaba/GraphScope/issues/new?assignees=&labels=&template=bug_report.md&title=%5BBUG%5D+) before starting to code. When submitting an issue, please include a clear and concise description of the problem, any relevant code or error messages, and steps to reproduce the issue.

## Request Feature

If you find yourself wishing for a feature that doesn't exist in GraphScope, you are probably not alone.
There are bound to be others out there with similar needs. Many of the features that GraphScope has today
have been added because our users saw the need.

[Open a feature request issue](https://github.com/alibaba/GraphScope/issues/new?assignees=&labels=&template=feature_request.md&title=) on GitHub which describes the feature you would
like to see, why you need it, and how it should work.

## Improve Documentation

A great way to contribute to the project is to improve documentation. If you found some docs to be incomplete or inaccurate, share your hard-earned knowledge with the rest of the community.

Documentation improvements are also a great way to gain some experience with our submission and review process, discussed below, without requiring a lot of local development environment setup. In fact, many documentation-only changes can be made directly in the GitHub web interface by clicking the “edit” button. This will handle making a fork and a pull request for you.

TBF: add a link to how to helping with documentation
TBF: add a link to how to build the documentation


## Contributing code and documentation changes

If you would like to contribute a new feature or a bug fix to GraphScope,
please discuss your idea first on the GitHub issue. If there is no GitHub issue
for your idea, please open one. It may be that somebody is already working on
it, or that there are particular complexities that you should know about before
starting the implementation. There are often a number of ways to fix a problem
and it is important to find the right approach before spending time on a PR
that cannot be merged.

### Install pre-commit

GraphScope use `pre-commit`_ to ensure no secrets are accidentally committed
into the Git repository, you could first install  `pre-commit`_ by

```bash
    $ pip3 install pre-commit
```

The configure the necessary pre-commit hooks with

```bash
    $ pre-commit install  --install-hooks
```

### Minor Fixes

Any functionality change should have a GitHub issue opened. For minor changes that
affect documentation, you do not need to open up a GitHub issue. Instead you can
prefix the title of your PR with "[MINOR] " if meets the following guidelines:

*  Grammar, usage and spelling fixes that affect no more than 2 files
*  Documentation updates affecting no more than 2 files and not more
   than 500 words.

### Fork & create a branch

You will need to fork the main GraphScope code and clone it to your local machine. See
[github help page](https://help.github.com/articles/fork-a-repo) for help.

Then you need to create a branch with a descriptive name.

A good branch name would be (where issue #42 is the ticket you're working on):

```bash
    $ git checkout -b 42-add-chinese-translations
```

### Get the test suite running

See [how to test](./how_to_test.md) for detail.

### Implement your fix or feature

At this point, you're ready to make your changes! Feel free to ask for help;
everyone is a beginner at first :smile_cat:

### Get the code format & style right

Your patch should follow the same conventions & pass the same code quality
checks as the rest of the project. Please follw [the code style guide](code_style_guide.md) to get the code format and style right.

### Submitting your changes

See [How to submit pull request](how_to_submit_pr)

### Discussing and keeping your Pull Request updated

You will probably get feedback or requests for changes to your pull request.
This is a big part of the submission process so don't be discouraged!
It is a necessary part of the process in order to evaluate whether the changes
are correct and necessary.

If a maintainer asks you to "rebase" your PR, they're saying that a lot of code
has changed, and that you need to update your branch so it's easier to merge.

To learn more about rebasing in Git, there are a lot of [good](http://git-scm.com/book/en/Git-Branching-Rebasing)
 [resources](https://help.github.com/en/github/using-git/about-git-rebase), but here's the suggested workflow:

```bash
    $ git checkout 42-add-chinese-translations
    $ git pull --rebase upstream main
    $ git push --force-with-lease 42-add-chinese-translations
```

Feel free to post a comment in the pull request to ping reviewers if you are awaiting an answer
on something. If you encounter words or acronyms that seem unfamiliar, refer to this [glossary](https://chromium.googlesource.com/chromiumos/docs/+/HEAD/glossary.md).

### Merging a PR (maintainers only)

A PR can only be merged into main by a maintainer if:

* It is passing CI.
* It has been approved by at least two maintainers. If it was a maintainer who
  opened the PR, only one extra approval is needed.
* It has no requested changes.
* It is up to date with current main.

### Shipping a release (maintainers only)

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
will pass ("be green") on all platform configurations supported by GraphAr.
This means that all tests pass and there are no linting errors. In reality,
however, it is not uncommon for the CI infrastructure itself to fail on specific
platforms ("be red"). It is vital to visually inspect the results of all failed ("red") tests
to determine whether the failure was caused by the changes in the pull request.

[1]: https://github.com/alibaba/GraphScope/blob/main/CODE_OF_CONDUCT.md

