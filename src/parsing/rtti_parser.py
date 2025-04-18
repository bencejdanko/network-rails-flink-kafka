from pyxb_bindings._for import CreateFromDocument as parse_ts_raw
from pyxb_bindings._sch3 import CreateFromDocument as parse_schedule_raw

def parse_ts_message(xml_string):
    try:
        obj = parse_ts_raw(xml_string)
        return obj
    except Exception as e:
        print(f"TS parsing failed: {e}")
        return None

def parse_schedule_message(xml_string):
    try:
        obj = parse_schedule_raw(xml_string)
        return obj
    except Exception as e:
        print(f"Schedule parsing failed: {e}")
        return None

if __name__ == "__main__":
    sample_ts_xml = """<?xml version="1.0" encoding="UTF-8"?>
<TS xmlns="http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3"
    rid="123456" uid="7890" ssd="2025-04-18">
    <Location tpl="ABC123" pta="12:30" et="12:35"/>
</TS>
"""

    sample_schedule_xml = """<?xml version="1.0" encoding="UTF-8"?>
<Schedule xmlns="http://www.thalesgroup.com/rtti/PushPort/Schedules/v3"
          rid="654321" uid="0987" trainId="1B23" ssd="2025-04-18" toc="VT" status="P" trainCat="OO">
    <OR tpl="XYZ789" wta="12:00" wtd="12:05" pta="12:00" ptd="12:05"/>
</Schedule>
"""

    ts_obj = parse_ts_message(sample_ts_xml)
    schedule_obj = parse_schedule_message(sample_schedule_xml)

    if ts_obj:
        print("TS Parsed Successfully")
        print("TS RID:", getattr(ts_obj, 'rid', None))
        if hasattr(ts_obj, 'Location') and ts_obj.Location:
            loc = ts_obj.Location[0]
            print("First TPL:", getattr(loc, 'tpl', None))
            print("PTA:", getattr(loc, 'pta', None))
            print("ET:", getattr(loc, 'et', None))
        else:
            print("No Location elements found.")

    if schedule_obj:
        print("Schedule Parsed Successfully")
        print("Schedule RID:", getattr(schedule_obj, 'rid', None))
        if hasattr(schedule_obj, 'OR') and schedule_obj.OR:
            or_loc = schedule_obj.OR[0]
            print("First OR TPL:", getattr(or_loc, 'tpl', None))
            print("WTA:", getattr(or_loc, 'wta', None))
            print("WTD:", getattr(or_loc, 'wtd', None))
        else:
            print("No OR elements found.")
