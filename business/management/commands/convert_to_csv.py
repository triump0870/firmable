from django.core.management.base import BaseCommand
import xml.etree.ElementTree as ET
import csv
import xml.dom.minidom

class Command(BaseCommand):
    help = 'Convert XML file to CSV with a limit of 100 records'

    # def add_arguments(self, parser):
    #     parser.add_argument('xml_file', type=str, help='Path to XML file')
    #     parser.add_argument('csv_file', type=str, help='Path to CSV file')

    def process_chunk(self, elements, csvwriter):
        # Process the chunk of elements here
        import pdb
        pdb.set_trace()
        for elem in elements:
            if elem.tag in ("FileSequenceNumber", "RecordCount", "ExtractTime", "TransferInfo"):
                continue

            if elem.tag == "ABN":
                abn = elem.text
                abn_status = elem.attrib.get('status')
                abn_status_from = elem.attrib.get('ABNStatusFromDate')
            elif elem.tag == "EntityTypeInd":
                entity_type = elem.text
            elif elem.tag == "EntityTypeText":
                entity_type_text = elem.text
            # main_entity = elem.find("MainEntity").text if elem.find("MainEntity") is not None else ""
            # legal_entity = elem.find("LegalEntity").text if elem.find("LegalEntity") is not None else ""
            # asic_number = elem.find("ASICNumber").text if elem.find("ASICNumber") is not None else ""
            # gst = elem.find("GST").text if elem.find("GST") is not None else ""
            # dgr = ", ".join([node.text for node in elem.findall("DGR")]) if elem.find("DGR") is not None else ""
            # other_entity = ", ".join([node.text for node in elem.findall("OtherEntity")]) if elem.find(
            #     "OtherEntity") is not None else ""
            #
            # # Write the extracted information to CSV
            # csvwriter.writerow([abn, entity_type, main_entity, legal_entity, asic_number, gst, dgr, other_entity])

    def handle(self, *args, **kwargs):
        xml_file = "/Users/rohanroy/Downloads/01.xml"
        csv_file = "output.csv"

        # Define the chunk size (number of elements to read at a time)
        chunk_size = 1000  # You can adjust this value as needed

        # Open CSV file in write mode
        with open(csv_file, "w", newline="") as csvfile:
            # Initialize CSV writer
            csvwriter = csv.writer(csvfile)

            # Write header row
            csvwriter.writerow(
                ["ABN", "EntityType", "MainEntity", "LegalEntity", "ASICNumber", "GST", "DGR", "OtherEntity"])

            # Iterate through the XML file in chunks
            with open(xml_file, "rb") as f:
                # Initialize the ElementTree iterparse iterator
                context = ET.iterparse(f, events=("start", "end"))

                # Initialize variables for keeping track of chunks
                chunk_count = 0
                elements = []

                # Iterate over events in the XML
                for event, elem in context:
                    if event == "start":
                        # Process the start of an element (if needed)
                        pass
                    elif event == "end":
                        # Process the end of an element
                        elements.append(elem)

                        # Check if the chunk size is reached
                        if len(elements) >= chunk_size:
                            # Process the chunk of elements
                            self.process_chunk(elements, csvwriter)

                            # Reset the elements list for the next chunk
                            elements = []
                            chunk_count += 1

                            # Clear the processed elements from memory
                            elem.clear()

                # Process any remaining elements (if the total number of elements is not a multiple of chunk_size)
                if elements:
                    self.process_chunk(elements, csvwriter)
