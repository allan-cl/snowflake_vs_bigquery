## Table of Contents

- [Setup](#setup)
- [Tests](#tests)
- [Reports](#reports)



## Setup
1. Install Miniconda
macOS and Linux:
Run the following commands in your terminal:
```bash
# macOS
curl -LO https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-x86_64.sh

# Linux
curl -LO https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Install
bash Miniconda3-latest-*-x86_64.sh
```
Follow the prompts in the installer. Remember to restart your terminal or run source ~/.bashrc (or equivalent for your shell) after installation.

2. Create a Conda Environment and activate it
```bash
conda create --name sf_vs_bq python=3.8
conda activate sf_vs_bq
```

3. Install Required Libraries
Navigate to the project directory and run:
```bash
pip install -r requirements.txt
```

## Tests
To run the tests for this project:
```bash
# Run setup tests
pytest tests/test_atc_setup.py --html=reports/test_atc_setup.html --benchmark-histogram=reports/test_atc_setup

# Run insert tests
pytest tests/test_atc_insert.py --html=reports/test_atc_insert.html

# Run query tests
pytest tests/test_atc_query.py --html=reports/test_atc_query.html --benchmark-histogram=reports/test_atc_query
```

## Reports

### Setup Tests

Click [HTML Report](./reports/test_atc_setup.html)

<img src="reports/test_atc_setup.svg" alt="test_atc_setup" width="600"/>

### Insert Tests
Click [HTML Report](./reports/test_atc_setup.html)

### Query Tests
Click [HTML Report](./reports/test_atc_query.html)

<img src="reports/test_atc_query.svg" alt="test_atc_query" width="600"/>


