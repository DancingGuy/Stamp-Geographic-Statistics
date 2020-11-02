# Stamp-Geographic-Statistics
Stamp_Regions.py parses eBay Stamps database using Finding API to aggregate statistics on the geographic areas of the world of all stamp lots offered for sale on the USA eBay site. There are 16 world regions as defined by eBay and the list will be in the README.

------------------------

1. What the project does

A single script file, in conjunction with a eBay YAML configuration file and the installed ebaysdk for python, will capture ensemble statistics from the CategoryContainer returned by eBay to assign counts to each of 16 categories from a defined list inclduing a catch-all category of Other. The results are written to disk as a flat file in CSV format, a text file of JSONs and also to a a MySQL database.

2. Why the project is useful

It demonstrates the power of the eBay python SDK to capture sitewide statistics of the stamp market so a seller can understand the level of competition faced against each region category.

. How users can get started with the project

First, you must obtain and install python, plus any modules in the import section of the main code file. Second, you must install the ebay python SDK. Third you must register as an eBay Developer to get access keys to make API calls into their database. Fourth you must set up your private ebay.yaml configuration file with your ebay Developer credentials. Fifth, you install a MySQL server. Lastly, you use a text editor or IDE to open the Stamp_Regions.py python file and run it after making any desired or necessary changes to (A) DEBUG on line 657, (B) absolute pathnames to the flat files in function openCSVFiles(), (C) change the MySQL credentials in function sqlHandshake(), (D) Setup the necessary MySQL schema and tables manually, or by reverse engineering the schema with the provided .mwb file.

4. Where users can get help with your project

My blogsite provides material on learning Python, SQL and MySQL Workbench, plus notes that explain certain fixes to the code as development progresses.

pythoncodedetective.blogspot.com

python.org and pypi.org for downloading and installing python plus any extra modules, along with help docs.

The ebaysk for python project is: https://github.com/timotheus/ebaysdk-python

Sign up for the free eBay Developers Program at: https://developer.ebay.com/signin?tab=register

Installation files fpr the MySQL serever are at: https://dev.mysql.com/

I use Notepad++ and the Thonny IDE for developing in Python and SQL. https://github.com/thonny/thonny
If you use Thonny, then I recommend installing the following packages or plugins from the Thonny Tools menu
a. friendly-traceback (debugging)
b. matplotlib (plotting)
c. numpy (arrays)
d. mysql-connector-python (required for database)
e. seaborn (plotting)

5. Who maintains and contributes to the project

John Quagliano
john_quagliano_phd@verizon.net


World Regions

Europe, United States, Great Britain, Topical Stamps, British Colonies & Territories, Worldwide, Asia, Canada, Australia & Oceania, Africa, Latin America, Middle East, Specialty Philately, Publications & Supplies, Caribbean, Other Stamps

