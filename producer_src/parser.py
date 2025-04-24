import pyxb
import pyxb_bindings._sch3 as sch3
import pyxb_bindings._for as for_bindings

def parse_schedule_xml(xml_string):
    #Parse the XML string into a Schedule object
    try:
        schedule = sch3.CreateFromDocument(xml_string)
        print("Parsed Schedule Object:\n", schedule)
        return schedule
    except Exception as e:
        print(f"Error parsing Schedule XML: {e}")
        return None

def parse_forecast_xml(xml_string):
    try:
        forecast = for_bindings.CreateFromDocument(xml_string)
        print("Parsed Forecast Object:\n", forecast)
        return forecast
    except Exception as e:
        print(f"Error parsing Forecast XML: {e}")
        return None

def extract_schedule_fields(schedule):
    if schedule is None:
        return None

    return {
        'rid': getattr(schedule, 'rid', None),
        'tpl': getattr(schedule, 'tpl', None),
        'wta': getattr(schedule, 'wta', None),
        'wtd': getattr(schedule, 'wtd', None),
        'wtp': getattr(schedule, 'wtp', None),
    }

def extract_forecast_fields(forecast):
    if forecast is None:
        return None

    return {
        'pta': getattr(forecast, 'pta', None),
        'ptd': getattr(forecast, 'ptd', None),
        'et': getattr(forecast, 'et', None),
        'at': getattr(forecast, 'at', None),
    }

def parse_and_extract(xml_string, message_type='schedule'):
    if message_type == 'schedule':
        schedule = parse_schedule_xml(xml_string)
        return extract_schedule_fields(schedule)
    elif message_type == 'forecast':
        forecast = parse_forecast_xml(xml_string)
        return extract_forecast_fields(forecast)
    else:
        print(f"Unknown message type: {message_type}")
        return None

