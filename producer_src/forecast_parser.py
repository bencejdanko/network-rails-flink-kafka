import pyxb
import pyxb_bindings._sch3
import pyxb_bindings._for

def parse_schedule_xml(xml_string):
    """Parse the XML string into a Schedule object."""
    try:
        schedule = pyxb_bindings._sch3.Schedule.FromXML(xml_string)
        return schedule
    except Exception as e:
        print(f"Error parsing Schedule XML: {e}")
        return None

def parse_forecast_xml(xml_string):
    """Parse the XML string into a Forecast object."""
    try:
        forecast = pyxb_bindings._for.Forecast.FromXML(xml_string)
        return forecast
    except Exception as e:
        print(f"Error parsing Forecast XML: {e}")
        return None

def extract_schedule_fields(schedule):
    """Extract relevant fields from the Schedule object."""
    if schedule is None:
        return None

    # Extract fields (handle potential None values gracefully)
    rid = schedule.rid if schedule.rid else None
    tpl = schedule.tpl if schedule.tpl else None
    wta = schedule.wta if schedule.wta else None
    wtd = schedule.wtd if schedule.wtd else None
    wtp = schedule.wtp if schedule.wtp else None

    # Return the extracted fields as a dictionary
    return {
        'rid': rid,
        'tpl': tpl,
        'wta': wta,
        'wtd': wtd,
        'wtp': wtp
    }

def extract_forecast_fields(forecast):
    """Extract relevant fields from the Forecast object."""
    if forecast is None:
        return None

    # Extract fields (handle potential None values gracefully)
    pta = forecast.pta if forecast.pta else None
    ptd = forecast.ptd if forecast.ptd else None
    et = forecast.et if forecast.et else None
    at = forecast.at if forecast.at else None

    # Return the extracted fields as a dictionary
    return {
        'pta': pta,
        'ptd': ptd,
        'et': et,
        'at': at
    }

def parse_and_extract(xml_string, message_type='schedule'):
    """Parse XML and extract the relevant fields based on message type."""
    if message_type == 'schedule':
        schedule = parse_schedule_xml(xml_string)
        return extract_schedule_fields(schedule)
    elif message_type == 'forecast':
        forecast = parse_forecast_xml(xml_string)
        return extract_forecast_fields(forecast)
    else:
        print(f"Unknown message type: {message_type}")
        return None
