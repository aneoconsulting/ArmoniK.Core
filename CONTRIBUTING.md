# Contributing Guide

Hi! We are very happy that you are interested in contributing to ArmoniK.Core project. Before submitting your contribution, please make sure to take a moment and read through the following guide.

## Pull Request Process

1. Make sure to do a PR against main
2. Make sure you clearly define the changes in the description of the PR
3. Make sure all tests pass by following the instructions available [here](.docs/content/0.installation/3.execute-tests.md)

## Release Process

When necessary, maintainers can release a new version. This new version will publish packages to registries.

### Release a new version

> Replace <version> with the new version number

1. Install .Net latest release version (https://dotnet.microsoft.com/en-us/download)
2. Make sure that ArmoniK.Api.* versions are releases
  1. `git grep '"ArmoniK\.Api\.'` to check which version is used. You can also see that with any editor in csproj files.
  2. If the versions are not a release, a PR should be opened to update them.
3. Make sure all tests pass by following the instructions available [here](.docs/content/0.installation/3.execute-tests.md)
4. Create a new release named `<version>` using the GitHub interface (be sure to select the main branch and to create a tag)

And _voilà_! The new version is released and a CI workflow will publish packages to registries.

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, gender identity and expression, level of experience,
nationality, personal appearance, race, religion, or sexual identity and
orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

- Using welcoming and inclusive language
- Being respectful of differing viewpoints and experiences
- Gracefully accepting constructive criticism
- Focusing on what is best for the community
- Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

- The use of sexualized language or imagery and unwelcome sexual attention or
  advances
- Trolling, insulting/derogatory comments, and personal or political attacks
- Public or private harassment
- Publishing others' private information, such as a physical or electronic
  address, without explicit permission
- Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project team at [contact@aneo.fr](mailto:contact@aneo.fr). All
complaints will be reviewed and investigated and will result in a response that
is deemed necessary and appropriate to the circumstances. The project team is
obligated to maintain confidentiality with regard to the reporter of an incident.
Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage], version 1.4,
available at [http://contributor-covenant.org/version/1/4][version]

[homepage]: http://contributor-covenant.org
[version]: http://contributor-covenant.org/version/1/4/
