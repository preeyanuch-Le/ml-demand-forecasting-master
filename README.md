# Pomelo Demand Forecasting 

## Project Stucture

The project is separated in /notebooks and /src folders.
* /notebooks contains all .ipynb files to run the pipeline on SageMaker
* /src contains all codes from preprocessing to modelling and postprocessing
* there are three notebooks [1]: data prep, [2] model train and [3] deployment both for online and offline 
* the credential to access Google API services are called from /config/project_secrets and should be stores in the project root in
  * /secrets/gdrive_secret.json
  * /secrets/gsheet_secret.json

## Steps prior to deploying

1. Create /secrets folder then put your gdrive and gsheet credential inside 
2. Rename credential to gdrive_secret.json and gsheet_secret.json
3. Create folder with model name inside temp/model_saves/online_model/ eg. df_model_25052022 and same for offline
4. Upload model file to the folder temp/model_saves/online_model/df_model_25052022 and same for offline

## To do
* [1]data_prep and [2] training notebooks need to be tested in the new folder structure
* add misc folder that contains all external excel files used in offline notebooks. 


# About seed repository for machine learning projects

This is a template for machine learning projects. It has been tested on:
* Ubuntu 20.04

Maintainers:
* Jussi J <jussi.j@pomelofashion.com>

## Project features

### Requirements

* Python 3.10.2 interpreter
* Poetry package manager

### Directory structure

* `/notebooks` Put your notebooks here
* `/src/pomelo/ml` Put your Python production grade code here. Define an appropriate
Python module namespace by creating nested directories, for example
`/src/pomelo/ml/similar_items/item2vec` which is translated as
`pomelo.ml.similar_items.item2vec` namespace, where
  * `pomelo`: indicates that this is a Pomelo Fashion project
  * `ml`: machine learning/data science project
  * `similar_items`: similar items project
  * `item2vec`: item2vec approach to find the similar items
* `/tests` Put your [pytest](https://docs.pytest.org/) unit tests here
* `/bin` Put your executables here

### Essential configuration files
* `/pyproject.toml` Defines general project configuration and Python dependencies
* `/Makefile` Defines helping project tasks to run locally or through Github actions
* `/Dockerfile` Configuration for wrapping the project into Docker images
* `/docker-compose.yml` Configuration for Docker compose
* `/.github/workflows/build.yml` Github action to build the project
* `/.github/workflows/deploy.yml` Github action to deploy the project
* `/.github/workflows/announce.yml` Github action to announce a new release in Slack
* `/.env` Application configuration
* `/.env.example` Example application configuration or configuration validation

Please update the project details in the files and uncomment where needed.
For more information, see below.

### Code quality

A set of code quality checking tools are pre-configured to run before committing code to
the git repository with `git commit` (via [pre-commit](https://pre-commit.com/)).
These tools include:

* [Black](https://black.readthedocs.io/en/stable/) code formatter
* [Flake8](https://flake8.pycqa.org/en/latest/) code linter
* [Mypy](https://mypy.readthedocs.io/en/stable/) static type checker

## Installation

Create a new repository for your project from this seed project template or integrate
with existing one. Do not clone this project as it is a template. Please change the
project name, authors and other relevant information in the files mentioned before.

It is recommended to use [pyenv](https://github.com/pyenv/pyenv) to set up a specific
version of the Python interpreter. Please follow the installation instructions for
`pyenv` and after that run the following from command line:
```commandline
pyenv install 3.10.2
pyenv shell 3.10.2
curl -sSL https://install.python-poetry.org | python3 -
```

This will install the specific Python interpreter version and Poetry package manager.

### Initialize project

Please get your personal [Github token](https://github.com/settings/tokens) and set
it at your shell first
```commandline
export GH_TOKEN=tokenid
```

At the root directory of your project, initialize the project development environment:

```commandline
make local/install
```

## Project development

### Entrypoint

Entrypoint of running data processing, model training, prediction or other tasks is
defined in `src/main.py`. Modify this file to suit the needs of your project. However,
depending on the type of the project, this entrypoint might not be needed at all
(e.g. package, POC), or it needs to be modified (e.g. Lambda function).

### Configuration

To ensure that Python modules of the project under `/src/pomelo` can be found by
the Python interpreter, set `PYTHONPATH` to point the absolute directory of
`/src`. For example, in the shell,
```commandline
export PYTHONPATH=$PYTHONPATH:$HOME/git/my-project/src
```
or through the run configuration of your IDE.

Environment dependent application configuration should be passed through environment
variables such as input and output data locations. These locations could be different
depending on whether the application is run locally, in a test environment or
production. In Python, configuration can be accessed with `os.environ["VARIABLE_NAME"]`.

Typically, the application configration is placed in the `.env` file at the project root
and is generated automatically in the CI/CD pipeline depending on the environment (e.g.
staging or production). You can provide configuration for your local environment
through this file. However, the file should and is ignored by git as configured in
`.gitignore`.

If you have configuration defined in Vault already, make run the following to generate
`.env` file:
```
export VAULT_ADDR=https://vault.pmlo.co
export VAULT_TOKEN=<your vault token>
export CONSUL_HTTP_ADDR=https://consul.pmlo.co:443
export CONSUL_HTTP_TOKEN=`vault kv get -field=operator_token kv/pmlo/services/consul`
export VAULT_APP_NAME=<your vault app name>
export STAGE=staging
make consul/generate-dot-env
```

This template includes the [Pomelo ML Utils](https://github.com/pomelofashion/python-pmlo-ml-utils) that provides tools for configuration, logging,
file handling and other. You may need to install additional libraries to use some of
the features.

### Run code

If applicable, run your code from the command line, for example:
```commandline
make local/run
```
which runs `src/main.py` as the entrypoint locally.

### Unit tests

Run unit tests with `pytest` in the `tests` directory:
```commandline
make local/test
```

### New package

Add new Python packages to the project with `poetry add <package-name>`. If the package
is needed only in the development phase, use `poetry add --dev <package-name>` instead.
See more instructions [here](https://python-poetry.org/docs/cli/). Make sure to lock the
package versions so that the packages are not updated for newer versions later unless
needed. This is important to ensure reproducibility.

### Code quality

You may run the tools defined in pre-commit directly as well.
For example:
* Sort imports `make local/isort`
* Lint code `make local/lint`
* Format code `make local/format`
* Type check code `make local/type`

## Set up access to AWS

In order to access AWS services, you need to set up your AWS credentials as
[environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html).

## Docker image

Prerequisites:
* [Docker](https://docs.docker.com/get-docker/)

Docker image is defined in the `Dockerfile` file. For example, `src/main.py` has been
configured to be ran from the entrypoint but this can be changed if needed. Please
change name of the image in `Makefile` to match your project first and use the following
commands:
* Build a Docker image: `make local/docker/build`
* Run Docker container: `make local/docker/run`
* Shell into the container: `make local/docker/shell`

AWS credentials are passed from the environment variables to the container.
To pass additional environment variables to the Docker container for testing, you need
to make the relevant changes to `Makefile`.

## VSCode

We recommended to use [VSCode](https://code.visualstudio.com/download) as an IDE.
After setting the environment variables, notably `PYTHONPATH`, run VSCode from
the root of your project with
```commandline
code .
```

Please install the following extensions to get started:
* Python
* Jupyter
* Jupytext

You need to configure VSCode to use the Python interpreter installed previously.
Make sure it uses the virtual environment set up by Poetry (typically in the
the `.venv` directory).

You can open, run and edit Jupyter notebooks in VSCode with the Jypyter extension.
Jupytext extension converts between Jupyter notebooks and Python files. Since Jupyter
notebooks do not have a git friend format, it is recommended not to push notebooks to
the git repo, but instead convert to Python files with Jupytext first. After pull, you
can convert the Python files back to notebook format by right-clicking the file and
selecting "Open as a Jupyter notebook".

Other useful extensions:
* Pylance
* GitLens
* Docker

## Deployment

The directory `.github/workflows` contains various task to build, deploy and release
the application. Please go through the files and place the application name where
appropriate. Do the same for `.github/starter-workflows` and copy the files to
`.github/workflows`. Currently, generating the `.env` configuration file is disable,
but uncomment it in case the application configuration has been placed to Vault.
