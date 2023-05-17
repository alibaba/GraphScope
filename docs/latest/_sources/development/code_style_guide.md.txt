# Code Style Guide

This document provides the coding style guidelines for GraphScope's codebase, which includes C++, Python, Rust, Java, and shell scripts.
Adhering to consistent coding standards across the codebase promotes maintainability, readability, and code quality.

## C++ Style

We follow the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) for coding standards in C++.

## Python Style

We follow the [black](https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html) code style for Python coding standards.

## Rust Style

We follow the [rust-lang code style](https://github.com/rust-lang/style-team/blob/master/guide/guide.md),
with the GraphScope custom [configuration](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/rustfmt.toml) for coding standards in Rust.

## Java Style

we follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html) for coding standards in Java.

## Script Style

We follow the [Google Shell Style Guide](https://google.github.io/styleguide/shellguide.html) for coding standards in shell script.

## Style Linter and Checker

GraphScope uses different linters and checkers for each language to enforce code style rules:

- C++: [clang-format-8](https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html) and [cpplint](https://github.com/cpplint/cpplint)
- Python: [Flake8](https://flake8.pycqa.org/en/latest/)
- Rust: [rust-fmt](https://github.com/rust-lang/rustfmt)
- Java: [google-java-format](https://github.com/google/google-java-format)

Each linter can be included in the build process to ensure that the code adheres to the style guide.
Below are the commands to check the code style in each language:

For C++, format and lint the code by the MakeFile command:

```bash
# format
$ make graphscope_clformat
# lint
$ make graphscope_cpplint
```

For Python:

- Install dependencies first:

```bash
$ pip3 install -r coordinator/requirements-dev.txt
```

- Check the style:

```bash
$ pushd python
$ python3 -m isort --check --diff .
$ python3 -m black --check --diff .
$ python3 -m flake8 .
$ popd
$ pushd coordinator
$ python3 -m isort --check --diff .
$ python3 -m black --check --diff .
$ python3 -m flake8 .
```

For Rust, we provide a shell script to do the format check:

```bash
$ cd interactive_engine/executor
$ ./check_format.sh
```

For Java:

-  Download the google-java-format tool: 

```bash
$ wget https://github.com/google/google-java-format/releases/download/v1.13.0/google-java-format-1.13.0-all-deps.jar
```

- Check the style:

```bash
$ files_to_format=$(git ls-files *.java)
$ java -jar google-java-format-1.13.0-all-deps.jar --aosp --skip-javadoc-formatting -i $files_to_format
```
