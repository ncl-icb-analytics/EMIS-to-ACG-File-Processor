"""
Configuration for the EMIS Web to Johns Hopkins ACG file processing.

This file defines the structure of input files, mappings to output files,
and any necessary data transformations.
"""
import pandas as pd # Needed for potential date transformations
from datetime import datetime

# --- Data Linking --- #
# Define the common column name used to link records across different input files.
# This MUST exist in all relevant input files.
MERGE_KEY = "PatientID"

# --- Input File Definitions --- #
# Define the expected columns for each of the 5 EMIS input files.
# *** Replace placeholder column names below with actual columns from EMIS extracts ***
INPUT_FILE_COLUMNS = {
    "Patient_Details": [
        "PatientID", "NHSNumber", "Age", "GenderCode", "Postcode",
        "Ethnicity", "LSOA", "PracticeCode", # Added potential pass-through vars
        # Add other actual columns from your Patient Details file...
    ],
    # "Appointments": [ # Removed as not used in current mapping
    #     "PatientID", "AppointmentDate", "AppointmentTime", "ClinicianID", "Status",
    #     # Add other actual columns from your Appointments file...
    # ],
    "Care_History": [ # Snomed code history, diagnoses, procedures etc.
        "PatientID", "Code", "CodeTerm", "EffectiveDate", "Value", "Unit",
        # Add other actual columns from your Care History file...
    ],
    "Medication_History": [
        "PatientID", "DrugCode", "DrugName", "IssueDate", "Quantity", "Dosage",
        # Add other actual columns from your Medication History file...
    ],
    "Long_Term_Conditions": [ # Often derived or specific LTC extracts
        "PatientID", "ConditionCode", "ConditionName", "OnsetDate", "ResolvedDate",
        # Add other actual columns from your LTC file...
    ],
}

# --- ACG Output File Specifications --- #
# Mappings are now defined in mapping.csv
# ACG_PATIENT_DATA_SPEC = { ... }
# ACG_MEDICAL_SERVICES_SPEC = { ... }
# ACG_PHARMACY_DATA_SPEC = { ... }

# --- Transformations --- #
# Define functions that can be referenced in mapping.csv.

# Removed calculate_age function as Age column is expected directly from input
# def calculate_age(dob_series):
#     ...

def transform_sex(sex_code_series):
    """Transforms Sex/Gender code. Input: Series."""
    # Assuming input might be 'M'/'F' or '1'/'2' etc. Adjust logic as needed.
    mapping = {'M': '1', 'F': '2', '1': '1', '2': '2'}
    # Use .map for efficient application, fill unknowns with '9'
    return sex_code_series.astype(str).str.upper().map(mapping).fillna('9')

def format_date_yyyy_mm_dd(date_series):
    """Formats date series to YYYY-MM-DD string, handling errors and NaT."""
    # Attempt to convert to datetime, coercing errors to NaT
    date_dt = pd.to_datetime(date_series, errors='coerce')

    # Create a series to store formatted dates, default to empty string
    formatted_dates = pd.Series('', index=date_series.index, dtype=str)

    # Identify successfully converted dates (not NaT)
    valid_dates_mask = date_dt.notna()

    # Apply strftime only to valid dates
    if valid_dates_mask.any():
        formatted_dates.loc[valid_dates_mask] = date_dt[valid_dates_mask].dt.strftime('%Y-%m-%d')

    return formatted_dates

def determine_dx_version(code_series):
    """Determines dx_version. Assumes all non-empty codes are SNOMED CT."""
    # Simplified logic: Return 'S' for any non-empty/non-null code.
    def get_version(code):
        if pd.isna(code) or str(code).strip() == '':
            return '' # Return empty if code is missing or just whitespace
        else:
            return 'S' # Assume SNOMED CT
    return code_series.apply(get_version)

def determine_rx_code_type(code_series):
    """Determines rx_code_type based on code characteristics (placeholder)."""
    # *** Placeholder Logic - Needs real implementation ***
    # Example: Based on source system, code format (BNF, dm+d etc.)
    # Valid ACG types: RRxUK, DMDUK, A (ATC), BRxUK
    # This MUST be accurate.
    def get_rx_type(code):
         if pd.isna(code) or code == '': return ''
         # Add actual logic here
         # if looks_like_dmd(code): return 'DMDUK'
         # if looks_like_bnf(code): return 'BRxUK'
         return 'RRxUK' # Default placeholder (assuming Read drug codes)
    return code_series.apply(get_rx_type)

def set_zero_cost(input_series):
    """Returns a series of empty strings (or zeros) for cost fields."""
    # ACG might prefer empty string '' or 0. Check spec.
    return pd.Series('0', index=input_series.index) # Or ''

def set_zero_utilization(input_series):
    """Returns a series of zeros (as strings) for utilization fields."""
    return pd.Series('0', index=input_series.index)

# --- Transformation Dictionary --- #
# Maps function names used in SPEC dictionaries to actual function objects
TRANSFORMATIONS = {
    # "calculate_age": calculate_age, # Removed
    "transform_sex": transform_sex,
    "format_date_yyyy_mm_dd": format_date_yyyy_mm_dd,
    "determine_dx_version": determine_dx_version,
    "determine_rx_code_type": determine_rx_code_type,
    "set_zero_cost": set_zero_cost,
    "set_zero_utilization": set_zero_utilization,
}

# --- Aggregations (Optional) --- #
# *** ACG processing is typically transactional. Review if pre-aggregation is needed. ***
# If needed, define aggregation logic AFTER ACG files are constructed (not usually done here).
# AGGREGATIONS = { ... }

# --- Output File Naming --- #
# Define how the final ACG output files should be named.
# Keys should match the target ACG file concepts.
OUTPUT_FILENAME_TEMPLATES = {
    "patient_data": "ACG_PatientData_{timestamp}.csv",
    "medical_services": "ACG_MedicalServices_{timestamp}.csv",
    "pharmacy_data": "ACG_PharmacyData_{timestamp}.csv",
} 