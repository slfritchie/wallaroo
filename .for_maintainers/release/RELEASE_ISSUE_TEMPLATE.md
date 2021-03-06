# Release Issue Template

This document is aimed at members of the Wallaroo team who might be cutting a release of Wallaroo. It serves as a template for the Release Issue used for testing the release candidate branch.

-----------------

Title Format: Wallaroo Release x.x.x Testing

This issue will serve as a checklist for the release testing process for Wallaroo x.x.x and for tracking any issues related to that release.

## Target Dates
### Release Target
We're targeting to release on the MM/DD.

### Phase 1 Target

Phase 1 should be completed by -- EST on DAY MM/DD

### Phase 2 Target

Phase 2 is expected to be complete by EOD DAY MM/DD

## Phase 1

Phase 1 consists of a complete test run of "Installing from Source" for Python + Go APIs, "Installing with Docker" for Python + Go APIs, "Installing with Vagrant" for Python + Go APIs, "Installing with Wallaroo Up" for Python + Go APIs, "Running an Application" for Python + Go for all installations, and running of example applications for all installations.

### Issues

Any issues that arise during testing should be created using the following format: `Release testing:` + issue description and should reference this issue and be added to the [x.x.x Milestone](https://github.com/WallarooLabs/wallaroo/milestone/). Issues are *NOT* to be addressed until Phase 2.

### Checklist

#### Docker for Python

##### Docker Installation Instructions

Follow the instructions for [setting up and installing Wallaroo](https://wallaroo-docs-rc.netlify.com/python-installation/python-docker-installation-guide/).

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

##### Docker "Run a Wallaroo Application" Instructions

Follow the instructions for [Run a Wallaroo Application in Docker](https://wallaroo-docs-rc.netlify.com/python-installation/python-docker-installation-guide/#validate-your-installation).

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

##### Docker Python Examples

Run through all the Python examples and make sure they work. This means everything in `examples/python`. You can run the automated version using `<wallaroo-install-path>/misc/example-tester.sh python`. This will run all examples except the kafka ones.

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

#### Python From Wallaroo Up

##### Installing From Wallaroo Up for Python Instructions

Follow the instructions for [setting up and installing Wallaroo](https://wallaroo-docs-rc.netlify.com/python-installation/python-wallaroo-up-installation-guide/).

- [ ] Ubuntu Xenial -
- [ ] Ubuntu Trusty -
- [ ] Ubuntu Bionic -
- [ ] Fedora 28 -
- [ ] Debian Stretch -
- [ ] CentOS 7 -

##### Python From Wallaroo Up "Run a Wallaoo Application" Instructions

Follow the instructions for the Python From Wallaroo Up [Run a Wallaroo Application](https://wallaroo-docs-rc.netlify.com/python-installation/python-wallaroo-up-installation-guide/#validate-your-installation).

- [ ] Ubuntu Xenial -
- [ ] Ubuntu Trusty -
- [ ] Ubuntu Bionic -
- [ ] Fedora 28 -
- [ ] Debian Stretch -
- [ ] CentOS 7 -

##### From Wallaroo Up Python Examples

Run through all the Python examples and make sure they work. This means everything in `examples/python`. You can run the automated version using `<wallaroo-install-path>/misc/example-tester.sh python`. This will run all examples except the kafka ones.

Making Sure it Works:
    - Building and running all  `examples/python` applications
    - verifying all applications build and run as expected

- [ ] Ubuntu Xenial -
- [ ] Ubuntu Trusty -
- [ ] Ubuntu Bionic -
- [ ] Fedora 28 -
- [ ] Debian Stretch -
- [ ] CentOS 7 -

#### Vagrant for Python

##### Vagrant Installation Instructions

Follow the instructions for [setting up and installing Wallaroo](https://wallaroo-docs-rc.netlify.com/python-installation/python-vagrant-installation-guide/).

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

##### Vagrant "Run a Wallaroo Application" Instructions

Follow the instructions for [Run a Wallaroo Application in Vagrant](https://wallaroo-docs-rc.netlify.com/python-installation/python-vagrant-installation-guide/#validate-your-installation).

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

##### Vagrant Python Examples

Run through all the Python examples and make sure they work. This means everything in `examples/python`. You can run the automated version using `<wallaroo-install-path>/misc/example-tester.sh python`. This will run all examples except the kafka ones.

- [ ] MacOS -
- [ ] Windows -
- [ ] Linux -

## Phase 2

Review all issues that arise from Phase 1. Address issues that we feel are worth including or can be included to meet release target date. Dependent on the issues addressed, another round of testing may be needed.

----
##  Vagrant Notes

If you are testing with Vagrant, you'll want to give at least 4 gigs of memory to virtualbox, this is done by adding the following to your Vagrantfile:

```ruby
  config.vm.provider "virtualbox" do |v|
    v.memory = 4084
  end
```
