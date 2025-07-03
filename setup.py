import os
import setuptools

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setuptools.setup(
    name = "xml_to_csv",
    version = "0.1.2",
    url = "https://github.com/kbrbe/xml-to-csv",
    author = "Sven Lieber",
    author_email = "Sven.Lieber@kbr.be",
    description = ("A Python script to extract XML fields to columns in CSV file(s). The script works in a streaming fashion and also has features to resolve 1:n relationships."),
    license = "MIT",
    keywords = "xml csv config json extraction transform",
    packages=setuptools.find_packages(),
    long_description_content_type = "text/markdown",
    long_description=read('README.md')
)
