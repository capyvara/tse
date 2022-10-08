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
- Run `scrapy crawl divulga` to update all the files
  - Scrapping is incremental and can be peformed continuously to get latest data if available, 
  - Files are downloaded to `data/download/...`

- Run `scrapy crawl urna` to download all the original files transmitted from the ballots (ballot bulletin, logs, etc), 
  - Beware that it's above 472k electoral sections with 6 files and about 200kb per section, totalling 2.8 million files 90gb

- Edit `divulgacao/setting.py` to customize paths, network usage, narrow down filters, etc.