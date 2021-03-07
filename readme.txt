CardioExtractor V1.1.0
First argument is a database file.
Second argument is an output directory for csv files.
Third argument is -v for the view mode, this mode will not extract any data, it will print records information only
Or the third argument can be -id with numbers following this flag.

Extract all ECG records to Output folder:

python3 main.py Data/TestDB.cDelver Output

View all records:

python3 main.py Data/TestDB.cDelver -v

Extract only tree ECG records with ID 1, 2 and 3 for the database:

python3 main.py Data/TestDB.cDelver Output -id 1 2 3
