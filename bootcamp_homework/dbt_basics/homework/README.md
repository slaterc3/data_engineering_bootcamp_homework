**dbt week 1 homework**
====================

This repository emulates an “open-source” project, though exclusively shared within the dataexpert community. Members can access the repository for independent use or contribute enhancements to the project's design and functionality. This serves as an opportunity to practice contributing to publicly shared open-source repositories.

**Table of Contents**


- [🚀 Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Local Development](#local-development)
  - [dbt Project Setup](#dbt-project-setup)
- [📥 Homework Submission](#-homework-submission)
- [📊 dbt Assignment](#-dbt-assignment)
  - [1 - Add new sources](#1---add-new-sources)
  - [2 - Create base models](#2---create-base-models)
  - [3 - Create a seed for valid email domains](#3---create-a-seed-for-valid-email-domains)
  - [4 - Create the Fact and Dimension tables](#4---create-the-fact-and-dimension-tables)
    - [4.1 - fact_visits](#41---fact_visits)
    - [4.2 - dim_haunted_houses](#42---dim_haunted_houses)
    - [4.3 - dim_customers](#43---dim_customers)
  - [5 (Optional) - Add custom tests](#5-optional---add-custom-tests)
    - [5.1 (Optional) - Custom generic test](#51-optional---custom-generic-test)
    - [5.2 (Optional) - Unit test](#52-optional---unit-test)
- [📄 List of Files Required](#-list-of-files-required)
  - [Sources](#sources)
  - [Staging](#staging)
  - [Seeds](#seeds)
  - [Marts](#marts)
  - [Tests (Optional)](#tests-optional)
  - [Macros (Optional)](#macros-optional)
- [📚 Other Helpful Resources for Learning!](#-other-helpful-resources-for-learning)
- [📂 Navigating the Repository](#-navigating-the-repository)


# **🚀 Getting Started**

## **Prerequisites**

1. **Python >= 3.9**

## **Local Development**

1. **Clone the Repository**: Open a terminal, navigate to your desired directory, and clone the repository using:
    ```bash
    git clone git@github.com:DataExpert-io/analytics-engineering-bootcamp-homework.git # clone the repo
    cd analytics-engineering-bootcamp-homework # navigate into the new folder
    ```

    1. If you don’t have SSH configured with the GitHub CLI, please follow the instructions for [generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) and [adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account?tool=cli) in the GitHub docs.



## dbt Project Setup

-  **Create a Branch:**
    - Navigate to the **`analytics-engineering-bootcamp-homework/dbt_basics/homework`** folder on your local machine.
    - Use the **`git checkout -b`** command to create a new branch where you can commit and push your changes. Prefix your branch name with your Git username to avoid conflicts.
          For example:

        ```bash
        git checkout -b homework/my-git-username
        ```
    - Create a copy of the **`template/`** folder and rename it to **`<your-git-username>`**, for example **`dbt_basics/homework/bruno`**.

- Go to the project's directory, assuming you are already in the **`homework`** folder:
  ```bash
  cd <your-git-username>
  ```
- Create a venv to isolate required packages
  ```bash
  python3 -m venv venv # MacOS/Linux
  # or
  python -m venv venv # Windows/PC
  ```
- Source the virtual environment
  ```bash
  source venv/bin/activate # MacOS/Linux
  # or
  venv/Scripts/activate # Windows/PC
  ```
- Install the required packages
  ```bash
  pip3 install -r dbt-requirements.txt # MacOS/Linux
  # or
  pip install -r dbt-requirements.txt # Windows/PC
  ```
- Update `DBT_SCHEMA` environment variable
  - MacOS/Linux:
    - Open the `dbt.env` file, change the `DBT_SCHEMA` to your schema from Weeks 1 and 2, and source the environment variables to your local (terminal) environment
      ```bash
      export DBT_SCHEMA='your_schema' # EDIT THIS FIELD
      ```
    - then run
      ```bash
      source dbt.env
      ```
  - Windows/PC:
    - Instead of overwriting the DBT_SCHEMA in the file you can run:
      - CMD:
      ```bash
      set DBT_SCHEMA=your_schema

      # For example
      set DBT_SCHEMA=bruno #(without quotes)
      ```
      - PowerShell:
      ```bash
      $env:DBT_SCHEMA = "your_schema"

      # For example
      $env:DBT_SCHEMA = "bruno"
      ```


- Run `dbt debug` to check your connection. You should see a message like this:
    ```
    21:43:08  Running with dbt=1.8.0
    21:43:08  dbt version: 1.8.0
    21:43:08  python version: 3.9.6
    21:43:08  python path: <path_to_repo>/airflow-dbt-project/dbt_project/venv/bin/python3
    21:43:08  os info: macOS-13.5.1-arm64-arm-64bit
    21:43:08  Using profiles dir at ./
    21:43:08  Using profiles.yml file at ./profiles.yml
    21:43:08  Using dbt_project.yml file at ./dbt_project.yml
    21:43:08  adapter type: trino
    21:43:08  adapter version: 1.8.0
    21:43:08  Configuration:
    21:43:08  profiles.yml file [OK found and valid]
    21:43:08  dbt_project.yml file [OK found and valid]
    21:43:08  Required dependencies:
    21:43:08   - git [OK found]

    21:43:08  Connection:
    21:43:08  host: dataengineer-eczachly.trino.galaxy.starburst.io
    21:43:08  port: 8443
    21:43:08  user: support@eczachly.com/student
    21:43:08  database: academy
    21:43:08  schema: <your schema>
    21:43:08  cert: None
    21:43:08  prepared_statements_enabled: True
    21:43:08  Registered adapter: trino=1.8.0
    21:43:10  Connection test: [OK connection ok]

    21:43:10  All checks passed!
    ```


You're good to go!


# 📊 dbt Assignment

**🎃 Welcome to the Haunted House Extravaganza! 👻🎟️**
Step into the data-filled world of thrills and chills as you venture into a theme park with 10 spine-tingling haunted houses!

🏚️ Your mission this week? Manage all the creepy customer tickets, terrifying feedback, and haunted house details lurking in the shadows. 💀

--

Remember that, if you are running your project for the first time, dbt will ask you to run

```bash
$ dbt deps
```

to install the packages defined in packages.yml

## Steps

### 1 - Add new sources

Inside the `models/staging` folder, create a source YAML file called `_sources.yml` and add the sources:
- `bootcamp.raw_customers`
- `bootcamp.raw_customer_feedbacks`
- `bootcamp.raw_haunted_houses`
- `bootcamp.raw_haunted_house_tickets`
to the `_sources.yml` file in the `models/staging` folder. (more info about sources here https://docs.getdbt.com/docs/build/sources)

### 2 - Create base models

Inside the `models/staging` folder, create a base model for each source (In dbt we call them staging models).
- `stg_customers.sql`
- `stg_customer_feedbacks.sql`
- `stg_haunted_houses.sql`
- `stg_haunted_house_tickets.sql`

These models should pull data from the sources. (use the `{{ source() }}` function we saw in the lab and lecture), and it should select all columns.

You can use the `codegen` function we saw in the lab to generate the base models (https://github.com/dbt-labs/dbt-codegen/tree/0.12.1/#create_base_models-source). Just change the name of the file later.

> :bulb: Note:
>
> You run your models by running in the terminal:
> - `dbt run` (To run all the models in the project)
>or
>- `dbt run -s stg_customers` (To run the specific model)
>

Inside the `models/staging` folder, create a YAML file for each model to include tests.
- `stg_customers.yml`
- `stg_customer_feedbacks.yml`
- `stg_haunted_houses.yml`
- `stg_haunted_house_tickets.yml`


Add at least `unique`and `not_null` tests to the primary keys. Feel free to add more tests to other columns. [More info about tests here](https://docs.getdbt.com/docs/build/data-tests#generic-data-tests).

> :bulb: Note:
>
> You test your models by running in the terminal:
> - `dbt test` (To test all the models in the project)
>or
>- `dbt test -s stg_customers` (To test the specific model)
>

> :bulb: Note:
>
> You run AND test your models by running in the terminal:
> - `dbt build` (To run AND test all the models in the project)
>or
>- `dbt build -s stg_customers` (To run AND test the specific model)
>


_(Optional) You can add documentation to your models. [Here's how to do it](https://docs.getdbt.com/docs/build/documentation#adding-descriptions-to-your-project)._

### 3 - Create a seed for valid email domains

As part of the logic of a _Dimension Table_ we will create in the next step, we want to validate the accepeted email domains for customers.

To be able to do it, we need to create a list of valid email domains. This is a good use-case for a seed. [More info about seeds here](https://docs.getdbt.com/docs/build/seeds).

Inside the **`seeds/`** folder, create a CSV file named **`**valid_domains.csv`**.

This CSV should have one single column name **`valid_domain`**, and two rows:
- `@example.com`
- `@example.io`

> :bulb: Note:
>
> You can run your seed by running in the terminal:
> - `dbt seed` (To run all the seeds in the project)
>or
>- `dbt seed -s valid_domains` (To run the specific seed)
>

### 4 - Create the Fact and Dimension tables

#### 4.1 - fact_visits
We will create a fact table to display information about a customer's visit to a haunted_house.

Inside the **`models/marts`** folder, create the fact_visits SQL file **`fact_visits.sql`**

Use your SQL skills to select the data from the models:
- `stg_haunted_house_tickets`
- `stg_customer_feedbacks`

When selecting from another model, use the `{{ ref('model_name') }}` function.

And join them to display all this information:
- `ticket_id`
- `customer_id`
- `haunted_house_id`
- `purchase_date`
- `visit_date`
- `ticket_type`
- `ticket_price`
- `rating`
- `comments`

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`fact_visits.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

#### 4.2 - dim_haunted_houses

Inside the **`models/marts`** folder, create the fact_visits SQL file **`dim_haunted_houses.sql`**

Use your SQL skills to select the data from the model:
- `stg_haunted_houses`

When selecting from another model, use the `{{ ref('model_name') }}` function.

Display all this information:
- `haunted_house_id`
- `house_name`
- `park_area`
- `theme`
- `fear_level`
- `house_size_in_ft2` (The original `house_size` columns)
- `house_size_in_m2` (read below)

_(Optional) The column `house_size_in_m2` can be calculated using a [macro](https://docs.getdbt.com/docs/build/jinja-macros). For that, you can follow the [dbt guide](https://docs.getdbt.com/docs/build/jinja-macros#macros) to declare your macro and call it in your model._

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`dim_haunted_houses.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

#### 4.3 - dim_customers

Inside the **`models/marts`** folder, create the fact_visits SQL file **`dim_customers.sql`**

Use your SQL skills to select the data from the model and seed:
- `stg_customers`
- `valid_domains`

When selecting from another model/seed, use the `{{ ref('model_name') }}` function.

Display all this information:
- `customer_id`
- `age`
- `gender`
- `email`
- `is_valid_email_address` (read below)

You will need to create a logic that uses the `valid_domains` seed to check if the customer has a valid email address.

A valid email address is an email:
- Containing a `@` separating the username from the domain.
- Containing a `.` in the domain.
- Having a domain listed in `valid_domains`.
[You can use this doc as referece to this validation, it is literally the same thing](https://docs.getdbt.com/docs/build/unit-tests#unit-testing-a-model).

--

Inside the **`models/marts`** folder, create the fact_visits YAML file **`dim_customers.yml`**.

Add at least `unique`and `not_null` tests to the primary key.

### (Optional) 5 - Add custom tests

#### (Optional) 5.1 Custom generic test
Inside the **`tests/generic`** folder, create file named **`is_positive.sql`**.

Inside this file, create a [generic test](https://docs.getdbt.com/best-practices/writing-custom-generic-tests) called `is_positive`, that asserts if the column has only positive values.

Add this test to the **`ticket_price`** column in **`models/staging/stg_haunted_house_tickets`**.



#### (Optional) 5.2 Unit test
Inside the **`models/marts/dim_customers.yml`** file, create a _unit test_ that checks if your logic to validate emails are right.

[You can use this doc as referece, it is literally the same thing](https://docs.getdbt.com/docs/build/unit-tests#unit-testing-a-model).

> :bulb: Just remember of including the example.io case too.
>



### 📄 List of files required:
Use this list to check if you have all the file for the homework.

#### sources
- `models/staging/_sources.yml`
#### staging
- `models/staging/stg_customer_feedbacks.sql`
- `models/staging/stg_customer_feedbacks.yml`
- `models/staging/stg_customers.sql`
- `models/staging/stg_customers.yml`
- `models/staging/stg_haunted_house_tickets.sql`
- `models/staging/stg_haunted_house_tickets.yml`
- `models/staging/stg_haunted_houses.sql`
- `models/staging/stg_haunted_houses.yml`
### seeds
- `seeds/valid_domains.csv`
#### marts
- `models/marts/fact_visits.sql`
- `models/marts/fact_visits.yml`
- `models/marts/dim_haunted_houses.sql`
- `models/marts/dim_haunted_houses.yml`
- `models/marts/dim_customers.sql`
- `models/marts/dim_customers.yml`
#### (Optional) tests
- `tests/generic/is_positive.sql`
- `models/marts/dim_customers.yml` (the unit test goes here)
#### (Optional) macros
- `macros/<the_name_you_want>.sql`

# 📚 Other helpful resources for learning!

### dbt docs
- [models](https://docs.getdbt.com/docs/build/sql-models)
- [tests](https://docs.getdbt.com/docs/build/data-tests)
- [sources](https://docs.getdbt.com/docs/build/sources)
- [seeds](https://docs.getdbt.com/docs/build/seeds)
- [snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt_project.yml](https://docs.getdbt.com/reference/dbt_project.yml)
- [profiles,yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml)
- [Commands](https://docs.getdbt.com/reference/commands/build)
- [Node selection](https://docs.getdbt.com/reference/node-selection/syntax)

### 📂 Navigating the Repository

Each dbt project contains various directories and files. Learn more about the structure of the project below:

- **`dbt_packages/`**: This folder is where dbt install packages (outside projects).
- **`logs/`**: This folder is where dbt store logs.
- **`macros/`**: This folder is where dbt searches for custom macros.
- **`models/`**: This folder is where dbt searches for models. You can create subfolders in the way you want, no problem.
- **`seeds/`**: This folder is where dbt searches for seeds.
- **`snapshots/`**: This folder is where dbt searches for snapshots.
- **`target/`**: This folder is where dbt stores [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) and the compiled SQL code (the code dbt sends to the data warehouse to run).
- **`tests/`**: This folder is where dbt searches for custom tests (generic or singular).
- **`dbt_project.yml`**: Every dbt project needs a dbt_project.yml file — this is how dbt knows a directory is a dbt project. It also contains important information that tells dbt how to operate your project. [More info here](https://docs.getdbt.com/reference/dbt_project.yml).
- **`packages.yml`**: This folder is where you define the packages you want dbt to install.