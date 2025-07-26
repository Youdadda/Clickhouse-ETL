from infi.clickhouse_orm import  Model, DateTimeField,StringField, MergeTree


class Winlogbeat(Model):
    timestamp = DateTimeField()
    winlog = StringField() 
    event_code = StringField("event.code")
    event_action = StringField("event.action")
    event_category = StringField("event.category")
    event_type = StringField("event.type")
    event_kind = StringField("event.kind")
    event_created = StringField("event.created")
    event_module = StringField("event.module")
    event_dataset = StringField("event.dataset")
    event_provider = StringField("event.provider")
    event_outcome = StringField("event.outcome")
    event_severity = StringField("event.severity")
    event_duration = StringField("event.duration")
    host = StringField()
    user = StringField()
    data = StringField()

    engine = MergeTree(
        order_by=('timestamp',),
        date_col='timestamp'  
                )
     


class m365(Model):
    pass


class SentinelOne(Model):
    pass

