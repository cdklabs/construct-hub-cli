# `construct-hub-cli`

The ConstructHub CLI is a command-line utility intended to help operators of a
ConstructHub instance diagnose and recover from issues, and to help developers
assess whether their package is eligible to be indexed in an instance of
ConstructHub or not (and if not, why).

> This utility is experimental and under active development. The commands it
> supports and the format of the data they return is subject to change without
> notice.

## `construct-hub repair-package-data`

This command traverses the *package data* bucket of a ConstructHub instance and
identified packages that are missing one or more language-specific documentation
object. Optionally, it will attempt to repair such packages by invoking the
Doc-Gen State Machine with the correct inputs.

## :cop: Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more
information.

## :balance_scale: License

This project is licensed under the Apache-2.0 License.
