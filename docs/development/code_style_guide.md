# Code Style Guide


This document describes the coding style guidelines for GraphScope's codebase.
Following consistent coding standards across the codebase ensures readability, maintainability, and code quality.

## C++ Style

We follow the [Google C++ Style Guide][1] for coding standards in C++.

## Python Style

We follow the [black][2] code style for Python coding standards.

## Rust Style

We follow a [custom style][3] for coding standards in Rust.

## Java Style

we follow the [Google Java Style Guide][4] for coding standards in Java.

## Script Style

We follow the [Google Shell Style Guide][5] for coding standards in shell script.

## Style Linter and Checker

GraphScope uses linters and checkers for each language to enforce code style rules.

- C++: [clang-format-8][6] and [cpplint][7]
- Python: [Flake8][8]
- Rust: [rust-fmt][9]
- Java: [google-java-format][10]

Each linter can be included in the build process to ensure that code adheres to the style guide.

For C++, format and lint the code by the MakeFile command:

```bash
    $ make graphscope_clformat
    $ make graphscope_cpplint
```

For Rust, we provide a shell script to do the format check:

```bash
    $ cd interactive_engine/executor
    $ ./check_format.sh
```

For Python:

```bash
    $ pip3 install -r coordinator/requirements-dev.txt
    $ pushd python
    $ python3 -m isort --check --diff .
    $ python3 -m black --check --diff .
    $ python3 -m flake8 .
    $ popd
    $ python3 -m isort --check --diff .
    $ python3 -m black --check --diff .
    $ python3 -m flake8 .
```

For Java:

```bash
    $ wget https://github.com/google/google-java-format/releases/download/v1.13.0/google-java-format-1.13.0-all-deps.jar
    $ files_to_format=$(git ls-files *.java)
    $ java -jar google-java-format-1.13.0-all-deps.jar --aosp --skip-javadoc-formatting -i $files_to_format
```

## Conclusion

In conclusion, following a consistent code style guide is crucial for maintaining code quality and readability. By adhering to these guidelines, we can ensure that code in GraphScope follows best practices and is easy to read, understand, and maintain. It is important to note that these guidelines are not set in stone and may evolve over time as the project grows and changes. Therefore, we encourage all contributors to review and suggest improvements to this document as needed.


[1]: https://google.github.io/styleguide/cppguide.html
[2]: https://github.com/psf/black
[3]: https://github.com/alibaba/GraphScope/blob/main/interactive_engine/executor/rustfmt.toml
[4]: https://google.github.io/styleguide/javaguide.html
[5]: https://google.github.io/styleguide/shellguide.html
[6]: https://releases.llvm.org/8.0.0/tools/clang/docs/ClangFormat.html
[7]: https://github.com/cpplint/cpplint
[8]: https://flake8.pycqa.org/en/latest/
[9]: https://github.com/rust-lang/rustfmt
[10]: https://github.com/google/google-java-format

