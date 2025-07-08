# Contributing to EdgeLink

Thank you for considering contributing to EdgeLink! We appreciate your interest and effort in helping to improve this project. This document outlines the guidelines and steps to follow when contributing to the project.

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [How Can I Contribute?](#how-can-i-contribute)
   - [Reporting Bugs](#reporting-bugs)
   - [Suggesting Enhancements](#suggesting-enhancements)
   - [Pull Requests](#pull-requests)
3. [Development Setup](#development-setup)
4. [Coding Guidelines](#coding-guidelines)
5. [Commit Guidelines](#commit-guidelines)
6. [Issue and Pull Request Labels](#issue-and-pull-request-labels)
7. [Community](#community)

## Code of Conduct

By participating in this project, you are expected to uphold our [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to me: oldrev@gmail.com.

## How Can I Contribute?

### Reporting Bugs

Before submitting a bug report, please check the [existing issues](https://github.com/oldrev/edgelink/issues) to see if the problem has already been reported. If it hasn't, you can create a new issue.

#### How Do I Submit a Good Bug Report?

- **Use a clear and descriptive title** for the issue to identify the problem.
- **Describe the exact steps** which reproduce the problem in as many details as possible.
- **Provide specific examples** to demonstrate the steps. Include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples.
- **Describe the behavior** you observed after following the steps and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected** to see instead and why.
- **Include screenshots or videos** which show you following the described steps and clearly demonstrate the problem.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement to an existing one, we'd love to hear about it! Here's how you can suggest enhancements:

#### How Do I Submit a Good Enhancement Suggestion?

- **Use a clear and descriptive title** for the issue to identify the suggestion.
- **Provide a step-by-step description** of the suggested enhancement in as many details as possible.
- **Describe the current behavior** and explain which behavior you expected to see instead and why.
- **Include screenshots and animated GIFs** which help you demonstrate the steps or point out the part of the project the suggestion is related to.
- **Explain why this enhancement would be useful** to most users.

### Pull Requests

1. **Fork the repository** and create your branch from `master`.
2. **Make your changes** in a new git branch:
   ```
   git checkout -b feature/your-feature-name
   ```
3. **Follow our coding guidelines** (see [Coding Guidelines](#coding-guidelines)).
4. **Ensure the test suite passes**.
5. **Make sure your code pass static checks, including `cargo fmt --all --check` and `cargo clippy --all`**.
6. **Commit your changes** using a descriptive commit message (see [Commit Guidelines](#commit-guidelines)).
7. **Push your branch** to GitHub:
   ```
   git push origin feature/your-feature-name
   ```
8. **Submit a pull request** to the `main` branch.

## Development Setup

To set up your development environment, follow these steps:

1. **Clone the repository**:
   ```
   git clone https://github.com/oldrev/edgelink.git
   ```
2. **Install dependencies for Python tests (Optional)**:
   ```
   cd edgelink
   pip -U -r ./tests/requirements.txt
   ```
3. **Run the CLI program**:
   ```
   cargo run
   ```

## Coding Guidelines

- **Follow the existing code style**. Consistency is key.
- **Write clear and concise code**. Comment your code where necessary.
- **Write tests** for your code and ensure all tests pass before submitting a pull request.

## Commit Guidelines

- Use the present tense ("Add feature" not "Added feature").
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
- Limit the first line to 72 characters or less.
- Reference issues and pull requests liberally after the first line.

## Issue and Pull Request Labels

We use labels to categorize issues and pull requests. Here are some of the labels you might see:

- **bug**: Something isn't working
- **enhancement**: New feature or request
- **documentation**: Improvements or additions to documentation
- **good first issue**: Good for newcomers
- **help wanted**: Extra attention is needed

## Community

Feel free to join our community discussions on [Discord](https://discord.gg/TODO). We're always happy to help and discuss new ideas!

---

Thank you for contributing to EdgeLink! Your efforts help make this project better for everyone.