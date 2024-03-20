# __Contribution Guidelines__
Welcome to _Dataverse_! We warmly welcome any kind of contribution 😊✨. </br>
This page provides an outline on how to contribute to _Dataverse_ and suggestions for nice conventions to follow. 
> __These are guidelines, NOT rules 💡__ <p>
This page is not the Constituion of the _Dataverse_. We are providing guidelines to help you make a useful and efficient contribution to _Dataverse_. While we think these guidelines are sensible and we appreciate when they are observed, following them isn't strictly required. We hope you won't be tired by these guidelines. Also, we'd love to hear your ideas on how to improve our guidelines! 

</br>

# Table of Contents
- [Questions or Feedback](#questions-or-feedback)
- [🤝 How to Contribute?](#how-to-contribute)
- [Tests](#tests)
- [Directory of Dataverse](#directory-of-dataverse)
- [Design Philosophy](#design-philosophy)
- [Commit Guidelines](#commit-guidelines)
- [Style Guides](#style-guides)

</br>

# Questions or Feedback
Join the conversation on our GitHub discussion board! It's the go-to spot for questions, chats, and a helping hand from the _Dataverse_ community. Drop by and say hello here: [link](https://github.com/UpstageAI/dataverse/discussions)

And if there's a shiny new feature you're dreaming of, don't be shy—head over to our [issue page](https://github.com/UpstageAI/dataverse/issues) to let us know! Your input could help shape the future. ✨

</br>

# How to Contribute?
- Any kind of improvement of document: fixing typo, enhancing grammar or semantic structuring or adding new examples.
- Submit issues related to bugs, new desired features, or enhancement of existing features.
- Fix a bug, implement new feature or improving existing feature.
- Answer other users' question or help.

## __Documentation__
We appreciate all the pull requests to fix typo / improve grammar or semantic structuring of documents. Feel free to check! <br/>
Our API reference page is constructed with [Sphinx](https://www.sphinx-doc.org/en/master/). We adhere to the [Google style for docstrings](https://google.github.io/styleguide/pyguide.html) as a fundamental practice, so please follow this format. The source files are located within the `docs/source/` directory.

## __Report a Bug / Request New Feature / Suggest Enhancements__
Please opne an issue whenever you find a bug or have an idea to enhance _Dataverse_. Maintainers will label it or leave comment on it as soon as they check the issue. Issues labeled as `Open for contribution` mean they are open for contribution.

## __Fix a Bug / Add New Feature / Improve Existing Feature__
If you have a particular roadmap, goals, or new feature, share it via issue. already fixed a bug or have new feature that enhacnes _Dataverse_, you can jump on to fourth step which is opening pull requests. Please note that when you open pull requests without opening an issue or maintainers' check, it can be declined if it does not aligh with philosophy of _Dataverse_.

### __1️⃣ Check issues labeled as__ `Open for contribution`
You can find issues waiting for your contribution by filtering label with `Open for contribution`. This label does not stand alone. It is always with `Bug`, `Docs` or `Enhancement`. Issues with `Critical` or `ASAP` label are more urgent. 


### __2️⃣ Leave a comment on the issue you want to contribute__
Once we review your comment, we'll entrust the issue to you by swapping out the `Open for contribution` label for a `WIP` (Work in Progress) label.

### __3️⃣ Work on it__
Before diving into coding, do take a moment to familiarize yourself with our coding style by visiting this [style guides](#style-guides). And hey, if you hit a snag while tackling the issue, don't hesitate to drop a comment right there. Our community is a supportive bunch and will jump in to assist or brainstorm with you.

1. Fork the repository of _Dataverse_.
2. Clone your fork to your local disk.
3. Create a new branch to hold your develompment changes. </br>
You don't need to follow branch naming example below.
```
git checkout -b {prefix}/{issue-number}-{description}
```
4. Set up a development environment
5. Develop the features in your branch


### __4️⃣ Create a Pull Request__
Go ahead and visit your GitHub fork, then initiate a pull request — it's time to share your awesome work! Before you do, double-check that you've completed everything on the checklist we provided. Once you're all set, submit your contributions for the project maintainers to review.

Don't worry if the maintainers have some feedback or suggest changes—it's all part of the process and happens to even our most experienced contributors. Keep your updates flowing by working in your local branch and pushing any new changes to your fork. Your pull request will update automatically for everyone to see the progress.

# Tests
The Dataverse test framework is built using [pytest](https://docs.pytest.org/en/8.0.x/). Ensure that you write a corresponding test for any new features or changes you make. You'll find the test files in the `dataverse/dataverse/tests` directory.

- Create a new test file if you've introduced a new category or a sub-category for the ETL process.
- If your addition is a new feature within an existing category or sub-category, include your tests in the existing test file.

# Directory of Dataverse
For _Dataverse_'s overarching goals: check the [docs](https://data-verse.gitbook.io/docs#future-work)

```{plain text}
📦 dataverse/dataverse
 ┣ 📂 api
 ┣ 📂 config
 ┃ ┣ 📂 etl
 ┃ ┃ ┗ 📂 sample
 ┣ 📂 etl
 ┃ ┣ 📂 {CATEGORY}
 ┣ 📂 lab
 ┣ 📂 tests
 ┗ 📂 utils
```
- [`📂 api`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/api): The Dataverse API serves as a
gateway for users.
- [`📂 config`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/config): Contains configuration files for the Dataverse application. You can also find sample configuration file for etl process under this directory.
- [`📂 etl`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/etl): Main directory of _Dataverse_ where all of the data processors are placed. Data processors are seperated with it's category.
- [`📂 lab`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/lab): TBD. Data analysis will be supported via here.
- [`📂 tests`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/tests): Pytest files 
- [`📂 utils`](https://github.com/UpstageAI/dataverse/tree/main/dataverse/utils): The Utilities module functions as a collection of internal helper tools. Its key features include API utilities that simplify interaction with various external APIs, including AWS EMR. Please be aware that another utils module is also included within the etl module.


# Design Philosophy
- [Principles for Configuration](#principles-for-configuration)
- [Principles for ETL Process](#principles-for-etl-process)

## Principles for Configuration
1. `One file` rules `ALL`
2. `10 Seconds` to know what is going on

#### 1. `One file` rules `ALL`
One cycle of ETL, Analyzer, etc. which we could call one job, will be controled by one configuration file. We are not going to use multiple configuration files to composite one big configuration file.

#### 2. `10 Seconds` to know what is going on
The reader should be able to know what is going on in the configuration file within 10 seconds. This is to make sure the configuration file is easy and small enough to read and understand.

## Principles for ETL Process
> When you create your own ETL process, you should follow the following principles

1. No `DRY` (Don't Repeat Yourself)
2. One file Only


#### 1. No `DRY` (Don't Repeat Yourself)
> No `DRY` is applied between **ETL sub-categories**.
- So if similar ETL processes are used in same sub-categories, it could be shared.
- But if it's used in different sub-categories, it should not be shared.

As you can see in the following example, there are 2 ETL processes `common_process_a` and `common_process_b`seems nice to be shared. But as you can see, they are not shared. They are repeated. This is because of the No `DRY` principle.


```python
- deduplication/
    - exact.py
        - "def common_process_a():"
        - "def common_process_b():"
        - def deduplication___exact___a():
    - exact_datasketch.py
        - "def common_process_a():"
        - "def common_process_b():"
        - def deduplication___exact_datasketch___a():
        - def deduplication___exact_datasketch___b():
```

#### 2. One file Only
Code that ETL process uses should be in the same file. This is because of the `One file Only` principle. Except **ETL Base class, few required utils functions, and open sources** there should be no dependency outside the file.

```python
# This is OK ✅
- deduplication/
    - exact.py
        - def helper_a():
        - def helper_b():
        - def etl_process():
            helper_a()
            helper_b()

                    
# This is not allowed ❌
- deduplication/
    - helper.py
        - def helper_a():
        - def helper_b():
    - exact.py
        from helper import helper_a
        from helper import helper_b

        - def etl_process():
            helper_a()
            helper_b()
```
ETL process itself is meant to be built to be used in various combination of ETL pipeline **So try to make it as generic as possible.** 
# Commit Guidelines
### Commit strategy
- Avoid mixing multiple, unrelated modifications in a single commit. One commit is related with one issue.
- Each commit should encapsulate a complete, autonomous upgrade to the code.

### Commit messages
Please make sure your commit messages follow `type`: `title (#<related issue number>)` format. <br/>
For example:
```plain text
<TYPE>: Short summary with 72 characters or less (#<Issue number>)

If you have more detalied explanatory text, put it as body.
But the body is optional.
```
- Find adequate type in the below list:
    - `NEW`: introducing a new feature
    - `ENHANCE`: improve an existing code/feature.
    - `FIX`: fix a code bug
    - `DOCS`: write/update/add any kind of documents including docstring
    - `REFACTOR`: refactor existing code without any specific improvements
    - `STYLE`: changes that do not affect the meaning of the code (ex. white-space, line length)
    - `TEST`: add additional testing
    - `DEL`: remove code or files
    - `RELEASE`: release new version of dataverse
    - `OTHER`: anything not covered above (not recommended)
- Use the present tense ("Add feature" not "Added feature")
- Do not end the subject line with a punctuation


# Style Guides
### Pre-commit hook
We provide a pre-commit git hook for style check. You can find exact check list in this [file](https://github.com/UpstageAI/dataverse/blob/main/.pre-commit-config.yaml). <br/> Please run the code below before a commit is created:
```bash
pre-commit run
```

