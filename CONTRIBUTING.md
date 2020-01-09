## Contributing to MarkLogic Content Pump

MarkLogic Content Pump (MLCP) and MarkLogic Connector for Hadoop (Hadoop Connector) welcome new contributors. This document will guide you through the process.

- [Got a question / Need help?](#got-a-question--need-help)
- [Issue/Bug Report or Feature Request](#issuebug-report-or-feature-request)
- [Submission Guidlines](#submission-guidlines)
	- [Submitting an Issue](#submitting-an-issue)
	- [Submitting a Pull Request](#submitting-a-pull-request)

## Got a question / Need help?

If you have questions about how to use MLCP or Hadoop Connector, you can ask on [StackOverflow](http://stackoverflow.com/questions/tagged/mlcp). Remember to tag the question with **mlcp** and **marklogic**. There are field experts monitoring tagged questions and ready to help!

## Issue/Bug Report or Feature Request

If you found a bug or ran into any issue in MLCP or Hadoop Connector or the documentations of these products, you can help us by submitting an issue to [GitHub Issue Tracker](https://github.com/marklogic/marklogic-contentpump/issues). You are also welcome to submit a pull request with a fix for the issue you filed.

If you would like to request a new feature, please submit an issue to [GitHub Issue Tracker](https://github.com/marklogic/marklogic-contentpump/issues). If you would like to implement a new feature then first create a new issue and discuss it with one of our project maintainers.

## Submission Guidlines

### Submitting an Issue

If your issue appears to be a bug, and hasn't been reported, open a new issue. Providing the following information will increase the chances of your issue being dealt with quickly:

- **Steps to reproduce the bug** - What is the MLCP command or Java program? Code snippet helps (and *Markdown* is our good friend)
- **Input and Output** - What is the input and expected output? What is the actual output (exception stack trace)?
- **Environment** - Operating System? MarkLogic Server version? MLCP (Hadoop Connector) version? Details help.
- **Suggest a fix** -  if you can't fix the bug yourself, perhaps you can point to what might be causing the problem (line of code or commit)

### Submitting a Pull Request

#### Fill in the CLA

Before we can accept your pull request, we need you to sign the [Contributor License Agreement](http://developer.marklogic.com/products/cla).

#### Fork the Repository

For the project on GitHub and clone your copy.
``` bash
$ git clone git@github.com:username/marklogic-contentpump
$ cd marklogic-contentpump
$ git remote add upstream git://github.com/marklogic/marklogic-contentpump.git
```
All bug fixes and new features go into the **develop** branch.

We ask that you open an issue in the issue tracker and get agreement from at least one of the project maintainers before you start coding. Nothing is more frustrating than seeing your hard work go to waste because your vision does not align with that of our engineering team.

#### Create a Branch for Your Changes

Okay, so you have decided to fix something. Create a feature branch and start hacking:

``` bash
$ git checkout -b my-feature-branch -t origin/develop
```

#### Commit Your Changes

Make sure git knows your name and email address:
``` bash
$ git config --global user.name "J. Random User"
$ git config --global user.email "j.random.user@example.com"
```
If you have a GitHub account please use the GitHub email. 

Writing good commit logs is important. A commit log should describe what changed and why. Follow these guidelines when writing one:

1. The first line should be 50 characters or less and contain a short description of the change including the Issue number prefixed by a hash (#).
2. Keep the second line blank.
3. Wrap all other lines at 72 columns.

A good commit log looks like this:

```
#123: make the whatchamajigger work in MarkLogic 8

Body of commit message is a few lines of text, explaining things
in more detail, possibly giving some background about the issue
being fixed, etc etc.

The body of the commit message can be several paragraphs, and
please do proper word-wrap and keep columns shorter than about
72 characters or so. That way `git log` will show things
nicely even when it is indented.
```

The header line should be meaningful; it is what other people see when they run `git shortlog` or `git log --oneline`.

#### Rebase Your Repository

Use `git rebase` (not `git merge`) to sync your work from time to time.

``` bash
$ git fetch upstream
$ git rebase upstream/develop
```

#### Test Your Change

Be sure to run the tests before submitting your pull request. Pull requrests with failing tests won't be accepted. The unit tests included in MLCP and Hadoop Connector are a minimum set of all the tests we have for the products. They are only designed for sanity check. To run unit tests, under marklogic-contentpump root directory:

```
$ mvn test
```

We will run large regression test sets against the change from the pull requests.

Please refer to wiki page [Guideline to Run Tests](https://github.com/marklogic/marklogic-contentpump/wiki/Guideline-to-Run-Tests) to learn about the details about running tests to verify your change.

#### Push Your Change

```bash
$ git push origin my-feature-branch
```

#### Submit the Pull Request

Go to [https://github.com/username/marklogic-contentpump](https://github.com/username/marklogic-contentpump) and select your feature branch. Click the 'Pull Request' button and fill out the form.

Pull requests are usually reviewed within a few days. If you get comments that need to be to addressed, apply your changes in a separate commit and push that to your feature branch. Post a comment in the pull request afterwards; GitHub does not send out notifications when you add commits to existing pull requests.

That's it! Thank you for your contribution!

#### After Your Pull Request is Merged

After your pull request is merged, you can safely delete your branch and pull 
the changes from the main (upstream) repository:

* Delete the remote branch on GitHub either through the GitHub web UI or your 
local shell as follows:

```bash
$ git push origin --delete my-feature-branch
```

* Check out the devevlop branch:

``` bash
$ git checkout develop -f
```

* Delete the local branch:

``` bash
$ git branch -D my-feature-branch
```

* Update your dev with the latest upstream version:

``` bash
$ git pull --ff upstream develop
```

