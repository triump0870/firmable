from sqlalchemy import Column, Integer, BigInteger, Text, Date, ForeignKey, UniqueConstraint, Enum, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from common import get_engine

# Create a database engine
engine = get_engine()

# Define a base class for declarative class definitions
Base = declarative_base()


# Define the Entities table
class ABNS(Base):
    __tablename__ = 'abns_main'

    abn = Column(BigInteger, primary_key=True)
    abn_status = Column(Text)
    abn_status_from_date = Column(Date)
    record_last_updated_date = Column(Date)
    replaced = Column(Text)
    entity_type_ind = Column(Text)
    entity_type_text = Column(Text)
    asic_number = Column(Text)
    asic_number_type = Column(Text)
    gst_status = Column(Text)
    gst_status_from_date = Column(Date)
    main_ent_type = Column(Text)
    main_ent_add_state = Column(Text)
    main_ent_add_postcode = Column(Text)
    legal_ent_type = Column(Text)
    legal_ent_title = Column(Text)
    legal_ent_family_name = Column(Text)
    legal_ent_given_names = Column(Text)
    legal_ent_add_state = Column(Text)
    legal_ent_add_postcode = Column(Text)

    __table_args__ = (
        UniqueConstraint('abn', 'record_last_updated_date', name='unique_abn_record_last_updated_date'),
    )


# Define the TradingNames table
class TradingName(Base):
    __tablename__ = 'trading_names_main'

    id = Column(Integer, primary_key=True)
    abn = Column(BigInteger, ForeignKey('abns_main.abn'))
    name = Column(Text)
    type = Column(Text)

    # Define a relationship with the Entities table
    entity = relationship("ABNS", back_populates="trading_names")

    __table_args__ = (
        UniqueConstraint('abn', 'name', 'type', name='unique_abn_name_type'),
    )


# Define the DGRStatus table
class DGRStatus(Base):
    __tablename__ = 'dgr_status_main'

    id = Column(Integer, primary_key=True)
    abn = Column(BigInteger, ForeignKey('abns_main.abn'))
    name = Column(Text)
    type = Column(Text)
    dgr_status_from_date = Column(Date)

    # Define a relationship with the Entities table
    entity = relationship("ABNS", back_populates="dgr_status")


# Define the FJCS table
class FJCS(Base):
    __tablename__ = 'fjcs_main'

    certificate_number = Column(Text, primary_key=True)
    abn = Column(BigInteger, ForeignKey('abns_main.abn'))
    entity_name = Column(Text)
    trade_name = Column(Text)
    status = Column(Text)
    issue_date = Column(Text)
    expiry_date = Column(Text)


# Define the AFSL table
class AFSL(Base):
    __tablename__ = 'afsl_main'

    afs_lic_num = Column(BigInteger, primary_key=True)
    afs_lic_abn_acn = Column(BigInteger)
    register_name = Column(Text)
    afs_lic_name = Column(Text)
    afs_lic_start_dt = Column(Text)
    afs_lic_pre_fsr = Column(Text)
    afs_lic_add_local = Column(Text)
    afs_lic_add_state = Column(Text)
    afs_lic_add_pcode = Column(Text)
    afs_lic_add_country = Column(Text)
    afs_lic_lat = Column(Text)
    afs_lic_lng = Column(Text)
    afs_lic_condition = Column(Text)


class Task(Base):
    __tablename__ = 'tasks'
    SOURCE_CHOICES = ['abns', 'fjcs', 'afsl']
    STATUS_CHOICES = ['Pending', 'InProgress', 'Completed']

    id = Column(Integer, primary_key=True)
    file_name = Column(Text)
    source = Column(Enum(*SOURCE_CHOICES, name='source_enum'))
    last_ran = Column(Date)
    status = Column(Enum(*STATUS_CHOICES, name='status_enum'))

    __table_args__ = (
        UniqueConstraint('source', 'last_ran', 'status', name='unique_source_last_ran_status'),
    )


# Add relationships to the Entity class
ABNS.trading_names = relationship("TradingName", back_populates="abn")
ABNS.dgr_status = relationship("DGRStatus", back_populates="abn")
ABNS.fjcs = relationship("FJCS", back_populates="abn")

# Define indexes on frequently used columns
abn_index = Index('abn_index', ABNS.abn)
issue_date_index = Index('issue_date_index', FJCS.issue_date)
