from django.core.management.base import BaseCommand
from business.models import Business
import xml.etree.ElementTree as ET
from django.db import transaction

class Command(BaseCommand):
    help = 'Import data from XML file into PostgreSQL database using Django models'

    def add_arguments(self, parser):
        parser.add_argument('xml_file', type=str, help='Path to XML file')

    def handle(self, *args, **kwargs):
        xml_file_path = kwargs['xml_file']

        # Function to extract data from XML and insert into database using Django models
        def xml_to_db(xml_file):
            tree = ET.parse(xml_file)
            root = tree.getroot()

            for transfer in root.findall('.//Transfer'):
                file_sequence_number = transfer.find('TransferInfo/FileSequenceNumber').text
                record_count = transfer.find('TransferInfo/RecordCount').text
                extract_time = transfer.find('TransferInfo/ExtractTime').text

                for abr in transfer.findall('ABR'):
                    abn = abr.find('ABN').text
                    entity_type_ind = abr.find('EntityType/EntityTypeInd').text
                    entity_type_text = abr.find('EntityType/EntityTypeText').text
                    asic_number = abr.find('ASICNumber').text if abr.find('ASICNumber') is not None else ''
                    gst_status = abr.find('GST').text if abr.find('GST') is not None else ''
                    dgr_status = abr.find('DGR').text if abr.find('DGR') is not None else ''
                    other_entity = abr.find('OtherEntity').text if abr.find('OtherEntity') is not None else ''

                    # Insert data into database
                    Business.objects.create(
                        abn=abn,
                        entity_type_ind=entity_type_ind,
                        entity_type_text=entity_type_text,
                        asic_number=asic_number,
                        gst_status=gst_status,
                        dgr_status=dgr_status,
                        other_entity=other_entity,
                        file_sequence_number=file_sequence_number,
                        record_count=record_count,
                        extract_time=extract_time
                    )

            self.stdout.write(self.style.SUCCESS('Data imported successfully'))

        # Import data from XML to database
        xml_to_db(xml_file_path)
