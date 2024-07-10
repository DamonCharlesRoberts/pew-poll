# PEW NORP

This is a little repository that I've pulled together based on some conversations that have been had on X about the party identification of 18-29 year olds in the 2024 PEW NROP study relative to what they had in 2023.

# Set up
1. Install poetry
2. Go to the current directory for this project on your computer.
3. Run the following in your terminal to install the dependencies.
```{bash}
poetry install
```

For the data staging and cleaning, run the following in your terminal
```{bash}
poetry run python 00_staging.py
poetry run python 01_creating_views.py
```

Now you can run the code in `03_analysis.py` to perform the analyses.