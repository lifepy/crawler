Files
=====

* **collect.py** Start from topmost seed urls of attraction list, scrape only links that might lead to attraction detail page.

* **crawl.py** Start from attraction detail page link and crawl for detailed attraction information, such as address,comments,rating,...etc.

* **db.py** Database connection file, also include db initialization code

* **manage.py** Management tool, could turn on collect/crawl/db sub-module via this interface
