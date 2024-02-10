import xml.etree.ElementTree as ET

# Define the input and output file paths
input_file = "/Users/rohanroy/Downloads/01.xml"
output_file = 'output.xml'

# Define the chunk size
chunk_size = 1000
from bs4 import BeautifulSoup


# Function to process and save a chunk of XML data
def process_chunk(data):
    # Write the chunk to the output file
    soup = BeautifulSoup(xml_data, "xml")
    z = zip(soup.select('ABR[recordLastUpdatedDate]'),
            soup.select('ABR[replaced]'),
            soup.select('ABN[status]'),
            soup.select('ABN[ABNStatusFromDate]'),
            soup.select('ABN'))

    for (c1, c2, c3, c4, c5) in z:
        print(c1['recordLastUpdatedDate'], c2['replaced'], c3['status'], c4['ABNStatusFromDate'], c5.text.strip())

# Read the XML file
with open(output_file, 'r') as file:
    xml_data = file.read()

process_chunk(xml_data)