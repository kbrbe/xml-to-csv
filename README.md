# XML-to-CSV

A Python script to extract XML fields to columns in CSV file(s). The script works in a streaming fashion and also has features to resolve 1:n relationships.

## Usage via the commandline

Create and activate a Python virtual environment

```bash

# Create a new Python virtual environment
python3 -m venv py-xml-to-csv-env

# Activate the virtual environment
source py-xml-to-csv-env/bin/activate

# Install dependencies
pip install -r requirements.txt
```

Afterwards the script can be executed via the commandline:

```bash
python -m xml_to_csv.xml_to_csv \
  -c config-example.json \
  -d date-mapping.json \
  -p "my-data" \
  -o "my-data.csv" \
  -l "my-data.log" \
  "my-input.xml"
```

For the config example the following files will be created

* `my-data.csv`
* `my-data-name.csv`
* `my-data-alternateNames.csv`
* `my-data-pseudonyms.csv`
* `my-data-birthDate.csv`
* `my-data-deathDate.csv`
* `my-data-isni.csv`

The file `my-date.csv` is the general output file in which every column besides the identifier column is an array containing possible 1:n relationships.
The other files contain 1:n relationships between each record and the values of a single column of the output.

### LICENSE

This script makes use of the following other software libraries.

| Library | Description | License |
|---------|-------------|---------|
| lxml | A library used to iterate fast (in a streaming fashion) over the input XML files | BSD |
| tqdm | A library to provide a user-friendly progress bar, used to show the progress of file extraction. | [MIT](https://opensource.org/licenses/MIT) |


## Contact

Sven Lieber - Sven.Lieber@kbr.be - Royal Library of Belgium (KBR) - https://www.kbr.be/en/

