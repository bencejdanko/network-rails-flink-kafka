# _for.py

Okay, let's break down this PyXB binding module (`_for.py`) for RTTI PushPort Forecasts v3.

This module defines the Python object structure corresponding to XML messages containing real-time train forecast updates according to the `http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3` namespace.

**Summary Report: Key Data Points in `_for.py` (RTTI PushPort Forecasts v3)**

The module primarily defines structures to represent **Train Status (TS)** updates, which provide real-time forecast information for specific train services.

**1. Train Service Identification and Overall Status (`TS` type):**
    *   **Service Identifiers:**
        *   `rid`: **RTTI unique Train Identifier**. The primary real-time ID for the service run. (Required)
        *   `uid`: **Train UID**. The schedule identifier for the train. (Required)
        *   `ssd`: **Scheduled Start Date**. The date the service is scheduled to start. (Required)
    *   **Service-Level Information:**
        *   `LateReason`: An optional element providing a coded reason for the train running late. This reason applies to the entire service. (Type: `DisruptionReasonType` from `_ct`)
        *   `isReverseFormation`: A boolean flag (default: false) indicating if a train that divides is operating with its portions reversed compared to the standard formation.

**2. Location-Specific Forecasts (`TSLocation` type within `TS`):**
    *   A `TS` message contains zero or more `Location` elements, each representing an update for a specific timing point in the train's schedule.
    *   **Location Identification:**
        *   `tpl`: **TIPLOC code**. Identifies the specific station or timing point. (Required)
    *   **Scheduled Times (Static Data):** These attributes define the *planned* schedule times at this location.
        *   `wta`: Working Time of Arrival.
        *   `wtd`: Working Time of Departure.
        *   `wtp`: Working Time of Pass.
        *   `pta`: Public Time of Arrival.
        *   `ptd`: Public Time of Departure.
    *   **Real-Time Event Forecasts/Actuals:** These elements contain detailed time and status updates for events at this location.
        *   `arr` (Arrival): Contains `TSTimeData` for arrival event. (Optional)
        *   `dep` (Departure): Contains `TSTimeData` for departure event. (Optional)
        *   `pass` (Pass): Contains `TSTimeData` for passing event (non-stopping). (Optional)
    *   **Platform Information (`plat` element of type `PlatformData`):** (Optional)
        *   *Value*: The platform number itself (inherited from `_ct.PlatformType`).
        *   `platsrc`: Source of the platform info ('P'lanned, 'A'utomatic, 'M'anual).
        *   `conf`: Boolean flag indicating if the platform is confirmed.
        *   `platsup`: Boolean flag indicating if the platform display is suppressed.
        *   `cisPlatsup`: Boolean flag indicating if suppression was set by a CIS/Darwin Workstation.
    *   **Other Location-Specific Status:**
        *   `suppr`: Boolean flag (default: false) indicating if the service is suppressed (e.g., cancelled call) at *this specific location*. (Optional)
        *   `length`: Train length (number of coaches) at this location (default: 0, meaning unknown). (Optional, Type: `TrainLengthType` from `_ct`)
        *   `detachFront`: Boolean flag (default: false) indicating if stock is detached from the *front* of the train at this location. (Optional)

**3. Time Data Details (`TSTimeData` type within `arr`, `dep`, `pass`):**
    *   This type holds the core forecast and actual time information for a specific event (arrival, departure, or pass) at a location. It has *no child elements*, only attributes.
    *   **Times:**
        *   `et`: **Estimated Time** (based on public schedule for public stops, working schedule otherwise).
        *   `wet`: **Working Estimated Time** (based on working schedule, provided if different from `et` or only operational activity exists).
        *   `at`: **Actual Time**.
    *   **Time Status Flags:**
        *   `delayed`: Boolean flag (default: false) indicating the current forecast is "Unknown Delay".
        *   `etUnknown`: Boolean flag (default: false) indicating a *manual* "Unknown Delay" forecast was set (the cause, distinct from the `delayed` status).
        *   `etmin`: A minimum estimated time (lower bound) manually applied.
        *   `atRemoved`: Boolean flag (default: false) set temporarily when an `at` is removed and replaced by an `et`.
    *   **Source Information:**
        *   `src`: String indicating the source system/method for the forecast/actual time.
        *   `srcInst`: The specific CIS instance code if the source (`src`) is a CIS system.
        *   `atClass`: String classification for the source of the actual time (`at`).

**In essence:**

This module defines the structure for receiving detailed, real-time updates about individual train services. Each update (`TS`) identifies a train (`rid`, `uid`, `ssd`) and provides potentially service-wide information (`LateReason`, `isReverseFormation`). The bulk of the data consists of updates for specific locations (`TSLocation`) along the route, including scheduled times (`wta`, `pta`, etc.), real-time forecasts/actuals for arrival/departure/pass events (`TSTimeData` containing `et`, `at`, `delayed` etc.), platform details (`PlatformData`), and other status flags like suppression or train length.


# _sch3.py

Okay, let's break down this PyXB binding module (`_sch3.py`) for the RTTI PushPort Schedules v3 schema.

**Analysis:**

1.  **Purpose:** This Python module is generated by PyXB from an XML Schema Definition (XSD). Its goal is to provide Python classes that directly map to the elements and types defined in the `http://www.thalesgroup.com/rtti/PushPort/Schedules/v3` namespace (aliased as `sch3`). This allows developers to easily parse XML documents conforming to this schema into Python objects and create/manipulate these objects to generate valid XML.
2.  **Source Schema:** The bindings are for the RTTI (Real Time Train Information) PushPort feed, specifically the "Schedules" part, version 3, provided by Thales Group.
3.  **Dependencies:** It imports other PyXB binding modules (`_ct`, `_ct2`, `_ct3`), likely containing common data types used across different parts of the RTTI schema (e.g., time types, location types, identifiers).
4.  **Core Structure:** The schema defines a primary structure `Schedule` which contains a sequence of "calling points". These calling points represent different types of stops or passing locations along a train's journey.
5.  **Calling Point Types:** The module defines distinct classes for different types of calling points:
    *   **Passenger vs. Operational:** Passenger points (`OR`, `IP`, `DT`) include public timings (`pta`, `ptd`), while Operational points (`OPOR`, `OPIP`, `OPDT`) focus on working timings (`wta`, `wtd`). Operational points are typically not shown on public displays but are relevant for railway operations.
    *   **Origin vs. Intermediate vs. Destination:** Differentiates the start (`OR`, `OPOR`), middle (`IP`, `OPIP`, `PP`), and end (`DT`, `OPDT`) points of a service or operational leg.
    *   **Stopping vs. Passing:** Stopping points (`OR`, `OPOR`, `IP`, `OPIP`, `DT`, `OPDT`) have arrival and departure times, while Passing points (`PP`) only have a passing time (`wtp`).

**Summary Report: Important Data Points Classified by `_sch3.py`**

This PyXB module provides Python bindings for parsing and creating train schedule data conforming to the RTTI PushPort Schedules v3 schema. It classifies the following key data points:

**I. Overall Schedule Information (`Schedule` class):**

*   **Identifiers:**
    *   `rid`: The unique RTTI identifier for this specific train run (Realtime ID).
    *   `uid`: The Train UID (Unique Identifier) from the base schedule.
    *   `trainId`: The operational Train ID, commonly known as the Headcode.
    *   `rsid`: Retail Service ID, used for linking related services or portions.
*   **Basic Details:**
    *   `ssd`: The Scheduled Start Date for the service.
    *   `toc`: The Train Operating Company code (ATOC code).
*   **Status & Characteristics:**
    *   `status`: The type of service (e.g., Train, Bus, Ship).
    *   `trainCat`: The category of service (e.g., Express Passenger, Ordinary Passenger).
    *   `isPassengerSvc`: Boolean flag indicating if it's a passenger service.
    *   `isCharter`: Boolean flag indicating if it's a charter service.
    *   `isActive`: Boolean flag indicating if the schedule is currently active in the Darwin system.
    *   `deleted`: Boolean flag indicating if the service has been deleted.
*   **Cancellation:**
    *   `cancelReason`: A structured element providing the code and textual reason if the entire service is cancelled.

**II. Calling Point Information (Classes: `OR`, `OPOR`, `IP`, `OPIP`, `PP`, `DT`, `OPDT`):**

These classes represent individual locations in the schedule (origins, intermediate points, destinations, passing points).

*   **Location:**
    *   `tpl`: The TIPLOC (Timing Point Location) code identifying the station or location. **(Universal)**
*   **Timing:**
    *   `wta`: Working Time of Arrival (for stopping points). **(Operational & Passenger Stopping)**
    *   `wtd`: Working Time of Departure (for stopping points). **(Operational & Passenger Stopping)**
    *   `wtp`: Working Time of Passing (for passing points). **(Passing Only)**
    *   `pta`: Public Time of Arrival (for passenger-facing stops). **(Passenger Stopping Only)**
    *   `ptd`: Public Time of Departure (for passenger-facing stops). **(Passenger Stopping Only)**
*   **Activities:**
    *   `act`: Activity codes defining what the train does at this location (e.g., "T" for stops, "U" for stops to pick up only, "D" for stops to set down only). **(Universal)**
    *   `planAct`: Planned activity codes, if different from the current ones (e.g., due to disruption). **(Universal)**
*   **Status:**
    *   `can`: Boolean flag indicating if this specific calling point is cancelled. **(Universal)**
*   **Train Formation:**
    *   `fid`: An identifier linking to detailed train formation (consist) data applicable *from* this location onwards. **(Universal)**
*   **Routing & Delay:**
    *   `rdelay`: A delay value (in minutes) attributed specifically to a re-route affecting arrival/passing at this location. **(Intermediate, Destination, Passing)**
    *   `fd`: False Destination TIPLOC - a different destination TIPLOC to be shown publicly at this location. **(Passenger Origin & Intermediate Only)**
*   **Loading:**
    *   `avgLoading`: An indication of the *average* long-term passenger loading expected for the train at this point (not real-time). **(Passenger Stopping Only)**

In essence, this module defines the structure for a detailed train schedule, including its identity, operator, status, and a sequence of timed and located events (arrivals, departures, passes) with associated activities and characteristics for both operational and public use.