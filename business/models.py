from django.db import models

# Create your models here.
from django.db import models


class Business(models.Model):
    abn = models.CharField(max_length=11, primary_key=True)
    abn_status_and_date = models.CharField(max_length=255)
    entity_type = models.CharField(max_length=255)
    legal_name = models.CharField(max_length=255)
    main_business_location = models.CharField(max_length=255)
    acn_arbn = models.CharField(max_length=255)
    gst_status_and_registration_date = models.CharField(max_length=255)
    deductible_gift_recipient_status_and_dates = models.CharField(max_length=255)


class BusinessName(models.Model):
    business = models.ForeignKey(Business, related_name='business_names', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)


class TradingName(models.Model):
    business = models.ForeignKey(Business, related_name='trading_names', on_delete=models.CASCADE)
    name = models.CharField(max_length=255)
