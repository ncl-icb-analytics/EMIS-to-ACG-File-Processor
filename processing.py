import pandas as pd
import os
import logging
import sys
from datetime import datetime
from collections import defaultdict
import config

logger = logging.getLogger(__name__)

def _apply_transformation(target_col_name, transform_key, source_df, source_col_name=None):
    """Applies a transformation function defined in config.TRANSFORMATIONS.

    Args:
        target_col_name (str): The name of the target ACG column being generated.
        transform_key (str): The key referencing the function in config.TRANSFORMATIONS.
        source_df (pd.DataFrame): The input DataFrame containing source data.
        source_col_name (str, optional): The specific source column to pass to the
                                         transformation function. If None, the index is passed.
                                         Defaults to None.

    Returns:
        pd.Series: The transformed data as a pandas Series, or an empty Series on error.
    """
    if transform_key not in config.TRANSFORMATIONS:
        logger.error(f"Transformation function '{transform_key}' for column '{target_col_name}' not found in config.TRANSFORMATIONS.")
        return pd.Series(dtype='object', index=source_df.index)

    transform_func = config.TRANSFORMATIONS[transform_key]

    # Determine the input for the transformation function
    if source_col_name:
        if source_col_name not in source_df.columns:
            logger.error(f"Source column '{source_col_name}' for transformation '{transform_key}' (target: '{target_col_name}') not found in source DataFrame.")
            return pd.Series(dtype='object', index=source_df.index)
        input_series = source_df[source_col_name]
    else:
        # Pass the DataFrame index if no specific column is needed (e.g., for set_zero_*)
        input_series = source_df.index

    try:
        logger.debug(f"Applying transformation '{transform_key}' for target column '{target_col_name}'.")
        result_series = transform_func(input_series)

        # Ensure the result is a Series of the correct length
        if not isinstance(result_series, pd.Series):
            try:
                # Attempt to create a Series if the function returned a scalar or list-like
                result_series = pd.Series(result_series, index=source_df.index)
            except Exception as e:
                 logger.error(f"Transformation '{transform_key}' did not return a Series for column '{target_col_name}' and couldn't be cast: {e}. Returned type: {type(result_series)}")
                 return pd.Series(dtype='object', index=source_df.index)

        # Check length consistency
        if len(result_series) != len(source_df.index):
            logger.error(f"Transformation '{transform_key}' for column '{target_col_name}' returned Series of wrong length ({len(result_series)} vs expected {len(source_df.index)}). Check transformation logic.")
            return pd.Series(dtype='object', index=source_df.index)

        return result_series

    except Exception as e:
        logger.exception(f"Error applying transformation '{transform_key}' for column '{target_col_name}': {e}")
        return pd.Series(dtype='object', index=source_df.index)

def _generate_patient_data(input_data_dict, mapping_df):
    """Generates the ACG Patient Data DataFrame based on mapping.csv.

    Args:
        input_data_dict (dict): Dictionary mapping input config keys to loaded DataFrames.
        mapping_df (pd.DataFrame): The loaded mapping DataFrame.

    Returns:
        pd.DataFrame or None: The generated Patient Data DataFrame, or None if essential data is missing.
    """
    logger.info("Generating ACG Patient Data file using mapping.csv...")
    required_input_key = "Patient_Details" # Patient data primarily comes from here

    if required_input_key not in input_data_dict:
        logger.error(f"Required input file '{required_input_key}' not found in provided data. Cannot generate Patient Data.")
        return None

    patient_details_df = input_data_dict[required_input_key].copy()

    # Validate merge key presence in the main patient details file
    if config.MERGE_KEY not in patient_details_df.columns:
         logger.error(f"Merge key '{config.MERGE_KEY}' not found in {required_input_key} data. Cannot generate Patient Data.")
         return None

    # Handle potential duplicate patient IDs
    if not patient_details_df[config.MERGE_KEY].is_unique:
         logger.warning(f"Duplicate PatientIDs found in {required_input_key}. Keeping first occurrence.")
         patient_details_df = patient_details_df.drop_duplicates(subset=[config.MERGE_KEY], keep='first')

    # Use the merge key column as the index for alignment during processing
    # Keep the column itself for potential direct mapping
    patient_details_df.set_index(config.MERGE_KEY, inplace=True, drop=False)

    # Filter mapping for patient_data file
    patient_mapping = mapping_df[mapping_df['TargetACGFile'] == 'patient_data'].copy()
    if patient_mapping.empty:
        logger.error("No mappings found for TargetACGFile='patient_data' in mapping.csv.")
        return None

    output_patient_df = pd.DataFrame(index=patient_details_df.index)

    # Process each mapping row for patient data
    for _, row in patient_mapping.iterrows():
        target_col = row['TargetACGColumn']
        input_key = row['InputConfigKey']
        input_col = row['InputColumn']
        transform_key = row['TransformationFunction']

        logger.debug(f"Processing Patient Data column: '{target_col}' from mapping.")
        result_series = pd.Series(dtype='object', index=output_patient_df.index)

        # Check if the specified input key matches the expected one for patient data
        if input_key != required_input_key:
             # Allow generation only transformations (InputColumn is blank)
             if pd.isna(input_col) or str(input_col).strip() == '':
                  if transform_key:
                      logger.debug(f"Applying generation transformation '{transform_key}' for target '{target_col}'.")
                      # Pass the base patient_details_df for index alignment
                      result_series = _apply_transformation(target_col, transform_key, patient_details_df, source_col_name=None)
                  else:
                       logger.warning(f"Mapping for '{target_col}' has no InputColumn and no TransformationFunction. Setting to empty.")
                       result_series = pd.Series('', index=output_patient_df.index)
             else:
                  logger.error(f"Mapping for '{target_col}' specifies InputConfigKey '{input_key}' but only '{required_input_key}' is supported for patient_data generation. Skipping.")
                  continue # Skip this column
        else:
             # Input source is Patient_Details
             source_df_for_transform = patient_details_df # Use the indexed DF
             source_col_for_transform = None

             # Check if input column is specified
             if not pd.isna(input_col) and str(input_col).strip() != '':
                 input_col = str(input_col).strip()
                 if input_col not in source_df_for_transform.columns:
                     logger.warning(f"InputColumn '{input_col}' for target '{target_col}' not found in {required_input_key}. Creating empty column.")
                     result_series = pd.Series('', index=output_patient_df.index)
                 else:
                      source_col_for_transform = input_col
                      # If no transformation, map directly
                      if not transform_key:
                           result_series = source_df_for_transform[input_col]
             elif not transform_key:
                  # No input column AND no transformation specified - this is ambiguous
                  logger.warning(f"Mapping for '{target_col}' has no InputColumn and no TransformationFunction. Setting to empty.")
                  result_series = pd.Series('', index=output_patient_df.index)

             # Apply transformation if specified (and not already directly mapped)
             if transform_key:
                 if source_col_for_transform or (not pd.isna(input_col) and str(input_col).strip() == ''): # Allow transform if source col exists OR if input col explicitly blank
                      result_series = _apply_transformation(target_col, transform_key, source_df_for_transform, source_col_name=source_col_for_transform)
                 # else: Error handled by input_col check above or ambiguous warning

        # Assign the processed or empty series to the output dataframe
        output_patient_df[target_col] = result_series

    # --- Finalize Patient Data --- #

    # Get column order from the mapping file for patient_data
    final_cols = patient_mapping['TargetACGColumn'].unique().tolist()

    # Ensure the merge key column ('patient_id') is present and first if mapped
    patient_id_col = 'patient_id' # Standard ACG name
    if patient_id_col in final_cols:
        final_cols.remove(patient_id_col)
        final_cols.insert(0, patient_id_col)
    elif config.MERGE_KEY in patient_details_df.columns: # Check if original merge key is available
         if config.MERGE_KEY not in output_patient_df.columns: # Add if not already mapped to something else
            logger.warning(f"Merge key '{config.MERGE_KEY}' was not mapped to '{patient_id_col}'. Adding it to output.")
            output_patient_df[config.MERGE_KEY] = patient_details_df[config.MERGE_KEY]
            if config.MERGE_KEY in final_cols: final_cols.remove(config.MERGE_KEY)
            final_cols.insert(0, config.MERGE_KEY)
         else: # Original key column was mapped - rename it to patient_id standard if needed
             if config.MERGE_KEY in final_cols:
                 final_cols[final_cols.index(config.MERGE_KEY)] = patient_id_col
             output_patient_df.rename(columns={config.MERGE_KEY: patient_id_col}, inplace=True)
             final_cols.insert(0, patient_id_col)

    # Reorder and check for missing columns
    try:
        output_patient_df = output_patient_df[final_cols]
    except KeyError as e:
        missing_mapped_cols = set(final_cols) - set(output_patient_df.columns)
        logger.error(f"Could not structure Patient Data output. Columns mapped but not generated: {missing_mapped_cols}. Error: {e}")
        return None # Fail generation if structure is wrong

    # Reset index before returning
    output_patient_df.reset_index(drop=True, inplace=True)

    logger.info(f"Generated Patient Data DataFrame with shape {output_patient_df.shape}")
    return output_patient_df

def _generate_medical_services(input_data_dict, mapping_df):
    """Generates the ACG Medical Services DataFrame based on mapping.csv.

    Args:
        input_data_dict (dict): Dictionary mapping input config keys to loaded DataFrames.
        mapping_df (pd.DataFrame): The loaded mapping DataFrame.

    Returns:
        pd.DataFrame or None: The generated Medical Services DataFrame, or None if no data is generated.
    """
    logger.info("Generating ACG Medical Services file using mapping.csv...")
    all_medical_dfs = []

    # Filter mapping for medical services
    medical_mapping = mapping_df[mapping_df['TargetACGFile'] == 'medical_services'].copy()
    if medical_mapping.empty:
        logger.error("No mappings found for TargetACGFile='medical_services' in mapping.csv.")
        return None

    # Identify unique source labels defined in the mapping for medical services
    source_labels = medical_mapping['SourceLabel'].unique()
    if len(source_labels) == 0 or (len(source_labels) == 1 and source_labels[0] == ''):
         logger.error("Mapping for 'medical_services' requires distinct 'SourceLabel' values. None found.")
         return None

    # Define standard ACG medical columns (can also derive this from mapping)
    target_columns = medical_mapping['TargetACGColumn'].unique().tolist()
    # Ensure essential columns like patient_id, dx_cd_1 are prioritized if present
    if 'patient_id' in target_columns: target_columns.insert(0, target_columns.pop(target_columns.index('patient_id')))
    if 'dx_cd_1' in target_columns: target_columns.insert(1, target_columns.pop(target_columns.index('dx_cd_1')))


    # Process each source defined by a SourceLabel
    for source_label in source_labels:
        if source_label == '': continue # Skip rows without a source label for multi-source files

        source_mapping = medical_mapping[medical_mapping['SourceLabel'] == source_label]

        # Determine the input config key for this source (should be consistent within a source label)
        input_keys = source_mapping['InputConfigKey'].unique()
        if len(input_keys) > 1:
            logger.error(f"SourceLabel '{source_label}' maps to multiple InputConfigKeys: {input_keys}. Skipping.")
            continue
        config_key = input_keys[0]

        if config_key not in input_data_dict:
            logger.warning(f"Input data for '{config_key}' (source: '{source_label}') not available. Skipping this source.")
            continue

        source_df = input_data_dict[config_key].copy()
        if source_df.empty:
             logger.warning(f"Input data for '{config_key}' (source: '{source_label}') is empty. Skipping.")
             continue

        logger.info(f"Processing medical source: '{source_label}' from input '{config_key}' ({source_df.shape[0]} rows)")

        # Create output DF for this specific source
        output_medical_df = pd.DataFrame(index=source_df.index)

        # Apply mappings for the current source
        for _, row in source_mapping.iterrows():
            target_col = row['TargetACGColumn']
            input_col = row['InputColumn'] # May be NaN/empty
            transform_key = row['TransformationFunction'] # May be NaN/empty

            logger.debug(f"Processing Medical Service column: '{target_col}' from source '{source_label}'")
            result_series = pd.Series(dtype='object', index=output_medical_df.index)
            source_col_for_transform = None

            # Check if input column is specified
            if not pd.isna(input_col) and str(input_col).strip() != '':
                input_col = str(input_col).strip()
                if input_col not in source_df.columns:
                    logger.warning(f"InputColumn '{input_col}' for target '{target_col}' (source: '{source_label}') not found in {config_key}. Creating empty column.")
                    result_series = pd.Series('', index=output_medical_df.index)
                else:
                    source_col_for_transform = input_col
                    # If no transformation, map directly
                    if not transform_key:
                        result_series = source_df[input_col]
            elif not transform_key:
                # No input column AND no transformation
                logger.warning(f"Mapping for '{target_col}' (source: '{source_label}') has no InputColumn and no TransformationFunction. Setting to empty.")
                result_series = pd.Series('', index=output_medical_df.index)

            # Apply transformation if specified
            if transform_key:
                # Determine the source column required by the transformation
                actual_source_col = source_col_for_transform # Default to the input col of the current mapping row

                # Special cases requiring a different source column than the one mapped for the current target
                if transform_key == 'determine_dx_version':
                    # Find the mapping row for dx_cd_1 within the same source label
                    dx_cd_mapping = source_mapping[source_mapping['TargetACGColumn'] == 'dx_cd_1']
                    if not dx_cd_mapping.empty:
                        dx_cd_input_col = dx_cd_mapping.iloc[0]['InputColumn']
                        # Ensure the found input column is valid
                        if pd.isna(dx_cd_input_col) or str(dx_cd_input_col).strip() == '' or dx_cd_input_col not in source_df.columns:
                             logger.error(f"Cannot apply '{transform_key}' for '{target_col}': InputColumn '{dx_cd_input_col}' for required 'dx_cd_1' mapping not found in {config_key} columns.")
                             actual_source_col = None # Prevent transform call
                        else:
                             actual_source_col = dx_cd_input_col # Use the InputColumn mapped to dx_cd_1
                    else:
                        logger.error(f"Cannot apply '{transform_key}' for '{target_col}': No mapping found for TargetACGColumn='dx_cd_1' within SourceLabel '{source_label}'.")
                        actual_source_col = None # Prevent transform call
                # Add similar logic here if other transformations need specific different source columns
                # elif transform_key == 'some_other_transform':
                #     needed_target = 'some_other_target' # e.g., find the mapping for 'target_col_x'
                #     needed_mapping = source_mapping[source_mapping['TargetACGColumn'] == needed_target]
                #     if not needed_mapping.empty: ... etc.

                # Call transformation if source column (or lack thereof for generation funcs) is valid
                # Allow transformation if actual_source_col is valid OR if input_col was explicitly blank (for generation funcs)
                can_transform = actual_source_col is not None and actual_source_col in source_df.columns
                is_generation_transform = pd.isna(input_col) or str(input_col).strip() == ''

                if can_transform or is_generation_transform:
                    # Use actual_source_col if found and needed, otherwise None for generation funcs
                    col_to_pass = actual_source_col if actual_source_col else None
                    result_series = _apply_transformation(target_col, transform_key, source_df, source_col_name=col_to_pass)
                else:
                     logger.warning(f"Skipping transformation '{transform_key}' for '{target_col}' due to missing/invalid required source column: '{actual_source_col}'.")
                     result_series = pd.Series('', index=output_medical_df.index)

            # Assign result to the temporary DataFrame for this source
            output_medical_df[target_col] = result_series

        # --- Finalize & Filter for this source --- #

        # Add any standard target columns if they weren't mapped for this source
        for std_col in target_columns:
            if std_col not in output_medical_df.columns:
                 logger.debug(f"Adding missing standard column '{std_col}' as empty for source '{source_label}'.")
                 output_medical_df[std_col] = ''

        # Ensure standard column order
        try:
            output_medical_df = output_medical_df[target_columns]
        except KeyError as e:
             missing_cols = set(target_columns) - set(output_medical_df.columns)
             logger.error(f"Could not structure Medical Services from source '{source_label}'. Columns missing: {missing_cols}. Error: {e}")
             continue # Skip this source if structure is wrong

        # Filter out rows missing essential info (patient_id, dx_cd_1)
        # Also filter based on dx_version if it exists and indicates exclusion (e.g., empty string from transformation)
        required_subset = ['patient_id', 'dx_cd_1']
        if 'dx_version_1' in output_medical_df.columns:
            output_medical_df = output_medical_df[output_medical_df['dx_version_1'].astype(str) != ''] # Filter out rows where code version indicates ignore

        output_medical_df.dropna(subset=required_subset, how='any', inplace=True)
        output_medical_df = output_medical_df[(output_medical_df['patient_id'].astype(str) != '') & (output_medical_df['dx_cd_1'].astype(str) != '')]

        if not output_medical_df.empty:
            logger.info(f"Processed medical source '{source_label}', resulting in {output_medical_df.shape[0]} valid rows.")
            all_medical_dfs.append(output_medical_df)
        else:
             logger.warning(f"No valid rows generated for medical source '{source_label}'.")

    # --- Concatenate results from all sources --- #
    if not all_medical_dfs:
        logger.error("No valid medical data generated from any source.")
        return None

    final_medical_df = pd.concat(all_medical_dfs, ignore_index=True)

    # Final check on column order (should be consistent, but double-check)
    try:
         final_medical_df = final_medical_df[target_columns]
    except KeyError:
         logger.error("Failed to ensure final column order for Medical Services.")
         # Decide if this is fatal, potentially return None

    logger.info(f"Generated final Medical Services DataFrame with shape {final_medical_df.shape}")
    return final_medical_df

def _generate_pharmacy_data(input_data_dict, mapping_df):
    """Generates the ACG Pharmacy Data DataFrame based on mapping.csv.

    Args:
        input_data_dict (dict): Dictionary mapping input config keys to loaded DataFrames.
        mapping_df (pd.DataFrame): The loaded mapping DataFrame.

    Returns:
        pd.DataFrame or None: The generated Pharmacy Data DataFrame, or None if no data is generated.
    """
    logger.info("Generating ACG Pharmacy Data file using mapping.csv...")
    all_pharmacy_dfs = []

    # Filter mapping for pharmacy data
    pharmacy_mapping = mapping_df[mapping_df['TargetACGFile'] == 'pharmacy_data'].copy()
    if pharmacy_mapping.empty:
        logger.warning("No mappings found for TargetACGFile='pharmacy_data' in mapping.csv. Pharmacy file will not be generated.")
        return None

    # Identify unique source labels
    source_labels = pharmacy_mapping['SourceLabel'].unique()
    if len(source_labels) == 0 or (len(source_labels) == 1 and source_labels[0] == ''):
         logger.error("Mapping for 'pharmacy_data' requires distinct 'SourceLabel' values. None found.")
         return None # Pharmacy data usually requires specifying the source

    # Define standard ACG pharmacy columns (can derive from mapping)
    target_columns = pharmacy_mapping['TargetACGColumn'].unique().tolist()
    # Prioritize standard columns
    if 'patient_id' in target_columns: target_columns.insert(0, target_columns.pop(target_columns.index('patient_id')))
    if 'rx_cd' in target_columns: target_columns.insert(1, target_columns.pop(target_columns.index('rx_cd')))
    if 'rx_fill_date' in target_columns: target_columns.insert(2, target_columns.pop(target_columns.index('rx_fill_date')))


    # Process each source defined by a SourceLabel
    for source_label in source_labels:
        if source_label == '': continue

        source_mapping = pharmacy_mapping[pharmacy_mapping['SourceLabel'] == source_label]
        input_keys = source_mapping['InputConfigKey'].unique()
        if len(input_keys) > 1:
            logger.error(f"SourceLabel '{source_label}' maps to multiple InputConfigKeys: {input_keys}. Skipping.")
            continue
        config_key = input_keys[0]

        if config_key not in input_data_dict:
            logger.warning(f"Input data for '{config_key}' (source: '{source_label}') not available. Skipping this source.")
            continue

        source_df = input_data_dict[config_key].copy()
        if source_df.empty:
             logger.warning(f"Input data for '{config_key}' (source: '{source_label}') is empty. Skipping.")
             continue

        logger.info(f"Processing pharmacy source: '{source_label}' from input '{config_key}' ({source_df.shape[0]} rows)")
        output_pharmacy_df = pd.DataFrame(index=source_df.index)

        # Apply mappings for the current source
        for _, row in source_mapping.iterrows():
            target_col = row['TargetACGColumn']
            input_col = row['InputColumn']
            transform_key = row['TransformationFunction']

            logger.debug(f"Processing Pharmacy Data column: '{target_col}' from source '{source_label}'")
            result_series = pd.Series(dtype='object', index=output_pharmacy_df.index)
            source_col_for_transform = None

            # Check if input column is specified
            if not pd.isna(input_col) and str(input_col).strip() != '':
                input_col = str(input_col).strip()
                if input_col not in source_df.columns:
                    logger.warning(f"InputColumn '{input_col}' for target '{target_col}' (source: '{source_label}') not found in {config_key}. Creating empty column.")
                    result_series = pd.Series('', index=output_pharmacy_df.index)
                else:
                    source_col_for_transform = input_col
                    if not transform_key:
                        result_series = source_df[input_col]
            elif not transform_key:
                logger.warning(f"Mapping for '{target_col}' (source: '{source_label}') has no InputColumn and no TransformationFunction. Setting to empty.")
                result_series = pd.Series('', index=output_pharmacy_df.index)

            # Apply transformation if specified
            if transform_key:
                actual_source_col = source_col_for_transform # Default to the input col of the current mapping row

                # Special case for rx_code_type needing rx_cd column:
                if transform_key == 'determine_rx_code_type':
                    rx_cd_mapping = source_mapping[source_mapping['TargetACGColumn'] == 'rx_cd']
                    if not rx_cd_mapping.empty:
                        rx_cd_input_col = rx_cd_mapping.iloc[0]['InputColumn']
                        if pd.isna(rx_cd_input_col) or str(rx_cd_input_col).strip() == '' or rx_cd_input_col not in source_df.columns:
                             logger.error(f"Cannot apply '{transform_key}' for '{target_col}': InputColumn '{rx_cd_input_col}' for required 'rx_cd' mapping not found in {config_key} columns.")
                             actual_source_col = None
                        else:
                             actual_source_col = rx_cd_input_col # Use the InputColumn mapped to rx_cd
                    else:
                        logger.error(f"Cannot apply '{transform_key}' for '{target_col}': No mapping found for TargetACGColumn='rx_cd' within SourceLabel '{source_label}'.")
                        actual_source_col = None

                # Call transformation if source column (or lack thereof for generation funcs) is valid
                can_transform = actual_source_col is not None and actual_source_col in source_df.columns
                is_generation_transform = pd.isna(input_col) or str(input_col).strip() == ''

                if can_transform or is_generation_transform:
                    col_to_pass = actual_source_col if actual_source_col else None
                    result_series = _apply_transformation(target_col, transform_key, source_df, source_col_name=col_to_pass)
                else:
                     logger.warning(f"Skipping transformation '{transform_key}' for '{target_col}' due to missing/invalid required source column: '{actual_source_col}'.")
                     result_series = pd.Series('', index=output_pharmacy_df.index)

            output_pharmacy_df[target_col] = result_series

        # --- Finalize & Filter for this source --- #
        for std_col in target_columns:
            if std_col not in output_pharmacy_df.columns:
                 logger.debug(f"Adding missing standard column '{std_col}' as empty for source '{source_label}'.")
                 output_pharmacy_df[std_col] = ''

        try:
            output_pharmacy_df = output_pharmacy_df[target_columns]
        except KeyError as e:
             missing_cols = set(target_columns) - set(output_pharmacy_df.columns)
             logger.error(f"Could not structure Pharmacy Data from source '{source_label}'. Columns missing: {missing_cols}. Error: {e}")
             continue

        # Filter out rows missing essential info (patient_id, rx_cd)
        required_subset = ['patient_id', 'rx_cd']
        output_pharmacy_df.dropna(subset=required_subset, how='any', inplace=True)
        output_pharmacy_df = output_pharmacy_df[(output_pharmacy_df['patient_id'].astype(str) != '') & (output_pharmacy_df['rx_cd'].astype(str) != '')]

        if not output_pharmacy_df.empty:
            logger.info(f"Processed pharmacy source '{source_label}', resulting in {output_pharmacy_df.shape[0]} valid rows.")
            all_pharmacy_dfs.append(output_pharmacy_df)
        else:
             logger.warning(f"No valid rows generated for pharmacy source '{source_label}'.")

    # --- Concatenate results --- #
    if not all_pharmacy_dfs:
        logger.warning("No valid pharmacy data generated from any source. Pharmacy file will not be created.")
        return None

    final_pharmacy_df = pd.concat(all_pharmacy_dfs, ignore_index=True)

    # Ensure final column order
    try:
        final_pharmacy_df = final_pharmacy_df[target_columns]
    except KeyError:
        logger.error("Failed to ensure final column order for Pharmacy Data.")

    logger.info(f"Generated final Pharmacy Data DataFrame with shape {final_pharmacy_df.shape}")
    return final_pharmacy_df

def generate_acg_files(input_data_dict, output_dir):
    """Main function to load mapping, generate the three ACG output files, and save them.

    Args:
        input_data_dict (dict): Dictionary where keys are input config keys
                                (e.g., "Patient_Details") and values are the
                                corresponding loaded pandas DataFrames.
        output_dir (str): The path to the directory where output files should be saved.
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Starting ACG file generation. Output directory: '{output_dir}'")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_files_generated = []
    generation_successful = True

    # --- Determine Base Path for Data Files (like mapping.csv) ---
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Running as packaged executable
        base_path = sys._MEIPASS
    else:
        # Running as script
        base_path = os.path.dirname(os.path.abspath(__file__))
    # --- End Base Path Determination ---

    # --- Load Mapping File --- #
    mapping_filename = 'mapping.csv'
    mapping_filepath = os.path.join(base_path, mapping_filename)
    mapping_df = None
    try:
        logger.info(f"Loading mapping configuration from: {mapping_filepath}")
        mapping_df = pd.read_csv(mapping_filepath, dtype=str)
        required_cols = ['InputConfigKey', 'InputColumn', 'TargetACGFile', 'TargetACGColumn']
        if not all(col in mapping_df.columns for col in required_cols):
            missing = set(required_cols) - set(mapping_df.columns)
            raise ValueError(f"Mapping CSV is missing required columns: {missing}")
        mapping_df['TransformationFunction'] = mapping_df['TransformationFunction'].fillna('')
        mapping_df['SourceLabel'] = mapping_df['SourceLabel'].fillna('')
        logger.info(f"Loaded {len(mapping_df)} mapping rows from {mapping_filename}")
    except FileNotFoundError:
        logger.error(f"CRITICAL: Mapping file '{mapping_filename}' not found at expected location: {mapping_filepath}. Cannot proceed.")
        raise RuntimeError(f"Mapping file '{mapping_filename}' not found at: {mapping_filepath}")
    except ValueError as ve:
        logger.error(f"CRITICAL: Invalid mapping file '{mapping_filename}': {ve}")
        raise RuntimeError(f"Invalid mapping file: {ve}")
    except Exception as e:
        logger.exception(f"CRITICAL: Failed to load or validate mapping file '{mapping_filename}': {e}")
        raise RuntimeError(f"Failed to load mapping file: {e}")

    # --- Generate Patient Data File --- #
    patient_df = _generate_patient_data(input_data_dict, mapping_df)
    if patient_df is not None and not patient_df.empty:
        output_filename = config.OUTPUT_FILENAME_TEMPLATES.get("patient_data", f"ACG_PatientData_{timestamp}.csv")
        output_filename = output_filename.replace("{timestamp}", timestamp)
        output_filepath = os.path.join(output_dir, output_filename)
        try:
            patient_df.to_csv(output_filepath, index=False, header=False, encoding='utf-8') # ACG often expects no header
            logger.info(f"Successfully saved Patient Data file: '{output_filepath}'")
            output_files_generated.append(output_filepath)
        except Exception as e:
            logger.exception(f"Error saving Patient Data file '{output_filepath}': {e}")
            generation_successful = False
    else:
        logger.error("Patient Data DataFrame was not generated or is empty. File not saved.")
        generation_successful = False # Mark as failed if essential file missing

    # --- Generate Medical Services File --- #
    medical_df = _generate_medical_services(input_data_dict, mapping_df)
    if medical_df is not None and not medical_df.empty:
        output_filename = config.OUTPUT_FILENAME_TEMPLATES.get("medical_services", f"ACG_MedicalServices_{timestamp}.csv")
        output_filename = output_filename.replace("{timestamp}", timestamp)
        output_filepath = os.path.join(output_dir, output_filename)
        try:
            medical_df.to_csv(output_filepath, index=False, header=False, encoding='utf-8') # ACG often expects no header
            logger.info(f"Successfully saved Medical Services file: '{output_filepath}'")
            output_files_generated.append(output_filepath)
        except Exception as e:
            logger.exception(f"Error saving Medical Services file '{output_filepath}': {e}")
            generation_successful = False
    else:
        logger.error("Medical Services DataFrame was not generated or is empty. File not saved.")
        generation_successful = False # Mark as failed if essential file missing

    # --- Generate Pharmacy Data File --- #
    pharmacy_df = _generate_pharmacy_data(input_data_dict, mapping_df)
    if pharmacy_df is not None and not pharmacy_df.empty:
        output_filename = config.OUTPUT_FILENAME_TEMPLATES.get("pharmacy_data", f"ACG_PharmacyData_{timestamp}.csv")
        output_filename = output_filename.replace("{timestamp}", timestamp)
        output_filepath = os.path.join(output_dir, output_filename)
        try:
            pharmacy_df.to_csv(output_filepath, index=False, header=False, encoding='utf-8') # ACG often expects no header
            logger.info(f"Successfully saved Pharmacy Data file: '{output_filepath}'")
            output_files_generated.append(output_filepath)
        except Exception as e:
            logger.exception(f"Error saving Pharmacy Data file '{output_filepath}': {e}")
            # Pharmacy is optional, so don't necessarily mark overall generation as failed
            # generation_successful = False
    else:
        logger.warning("Pharmacy Data DataFrame was not generated or is empty. File not saved (Optional File).")

    logger.info(f"ACG file generation finished. {len(output_files_generated)} files generated.")

    # Raise an error if essential files weren't generated
    if not generation_successful:
         logger.error("One or more essential ACG files (Patient, Medical) were not generated successfully.")
         # This exception will be caught by the worker thread in main.py
         raise RuntimeError("Failed to generate essential ACG files. Check logs for details.")

# Example usage (for testing - requires config updates)
if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
         logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logger.info("Running processing.py directly for testing...")

    # Create dummy input files based on new config keys
    test_input_dir = 'test_input'
    test_output_dir = 'test_output'
    os.makedirs(test_input_dir, exist_ok=True)
    os.makedirs(test_output_dir, exist_ok=True)

    # Dummy data generation helper (ensure all configured cols exist)
    def create_dummy_df(cols, data):
        df = pd.DataFrame(data)
        for col in cols:
            if col not in df.columns:
                df[col] = '' # Add missing configured columns as empty
        return df[cols] # Ensure column order matches config

    try:
        patient_cols = config.INPUT_FILE_COLUMNS.get("Patient_Details", [])
        # appt_cols = config.INPUT_FILE_COLUMNS.get("Appointments", []) # Removed
        care_cols = config.INPUT_FILE_COLUMNS.get("Care_History", [])
        med_cols = config.INPUT_FILE_COLUMNS.get("Medication_History", [])
        ltc_cols = config.INPUT_FILE_COLUMNS.get("Long_Term_Conditions", [])

        # Generate dummy data files
        # Note: Using Age column now based on config change
        dummy_patient_data = {'PatientID': ['1', '2'], 'NHSNumber': ['N1', 'N2'], 'Age': ['38', '31'], 'GenderCode': ['M', 'F'], 'Postcode':['P1','P2'], 'Ethnicity':['E1','E2'], 'LSOA':['L1','L2'], 'PracticeCode':['PC1','PC2']}
        create_dummy_df(patient_cols, dummy_patient_data).to_csv(os.path.join(test_input_dir, 'EMIS_Patient_Details.csv'), index=False)

        # dummy_appt_data = {'PatientID':['1','1','2'], 'AppointmentDate':['2023-01-10','2023-02-15','2023-01-20'], 'ClinicianID':['C1','C2','C1'], 'Status':['Seen','Seen','DNA']}
        # create_dummy_df(appt_cols, dummy_appt_data).to_csv(os.path.join(test_input_dir, 'EMIS_Appointments.csv'), index=False) # Removed

        dummy_care_data = {'PatientID':['1','1','2','1'], 'Code':['G30..','H54..','XE0Uc','F222.'], 'CodeTerm':['Alzheimer','Blindness','Asthma','Diabetes'], 'EffectiveDate':['2020-01-01','2021-05-10','2022-03-15','2023-01-01']}
        create_dummy_df(care_cols, dummy_care_data).to_csv(os.path.join(test_input_dir, 'EMIS_Care_History.csv'), index=False)

        dummy_med_data = {'PatientID':['1','2','2'], 'DrugCode':['a123.','b456.','c789.'], 'DrugName':['DrugA','DrugB','DrugC'], 'IssueDate':['2023-01-15','2023-02-20','2023-02-28']}
        create_dummy_df(med_cols, dummy_med_data).to_csv(os.path.join(test_input_dir, 'EMIS_Medication_History.csv'), index=False)

        dummy_ltc_data = {'PatientID':['1'], 'ConditionCode':['LTC01'], 'ConditionName':['Hypertension'], 'OnsetDate':['2019-01-01'], 'ResolvedDate':['']}
        create_dummy_df(ltc_cols, dummy_ltc_data).to_csv(os.path.join(test_input_dir, 'EMIS_Long_Term_Conditions.csv'), index=False)

        # Load dummy data into dictionary for processing function
        input_dict_test = {}
        # Simple filename matching for test setup
        for filename in os.listdir(test_input_dir):
            filepath = os.path.join(test_input_dir, filename)
            matched_key = None
            if 'Patient_Details' in filename: matched_key = "Patient_Details"
            # elif 'Appointments' in filename: matched_key = "Appointments" # Removed
            elif 'Care_History' in filename: matched_key = "Care_History"
            elif 'Medication_History' in filename: matched_key = "Medication_History"
            elif 'Long_Term_Conditions' in filename: matched_key = "Long_Term_Conditions"

            if matched_key:
                try:
                     input_dict_test[matched_key] = pd.read_csv(filepath, dtype=str)
                     logger.debug(f"Test loading: Loaded {filename} as {matched_key}")
                except Exception as e:
                     logger.error(f"Test loading: Failed to load {filename}: {e}")
            else:
                logger.warning(f"Test loading: Could not match filename {filename} to config key.")

        missing_test_files = set(config.INPUT_FILE_COLUMNS.keys()) - set(input_dict_test.keys())
        if missing_test_files:
             logger.error(f"Test setup failed: Missing loaded dummy files for {missing_test_files}")
        elif not input_dict_test:
             logger.error("Test setup failed: No dummy input files were loaded.")
        else:
             logger.info("Test setup complete, calling generate_acg_files...")
             try:
                # Call the main generation function
                generate_acg_files(input_dict_test, test_output_dir)
                print(f"\nTest processing complete. Check the '{test_output_dir}' directory for ACG files.")
             except Exception as e:
                  logger.exception("Error during testing generate_acg_files.")
                  print(f"\nTest processing failed: {e}")

    except AttributeError as e:
        logger.error(f"Config error during test setup: {e}. Cannot run test.")
    except Exception as e:
         logger.exception(f"General error during test setup: {e}")

    # Clean up dummy files (optional)
    # import shutil
    # shutil.rmtree(test_input_dir)
    # shutil.rmtree(test_output_dir)
 