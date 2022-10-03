# TSE data utilities


Scraps all the from the election results from Tribunal Superior Eleitoral files at:

https://www.tse.jus.br/eleicoes/eleicoes-2022/interessados-na-divulgacao-de-resultados-2022

Please read their policy and docs before using the tool.

# Prerequisites
- Python 3.6+
- Python Poetry: https://python-poetry.org/docs/#installation 
  - Or use `brew install poetry`
- Install the dependencies `poetry install`
- Activate shell/virtual environment `poetry shell`

# Usage
- Run `scrapy divulga` to update all the files, scrapping is incremental and can be peformed continuously to get latest data if available, files are downloaded to `data/download/...`